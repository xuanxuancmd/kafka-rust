// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! ConnectorContext for use with a Herder.
//!
//! This provides a connector context implementation that works with a Herder
//! to handle task reconfiguration requests and error handling.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.HerderConnectorContext` in Java.

use crate::runtime::closeable_connector_context::CloseableConnectorContext;
use common_trait::metrics::PluginMetrics;
use connect_api::connector::ConnectorContext;
use connect_api::errors::ConnectError;
use log::warn;

/// ConnectorContext for use with a Herder.
///
/// This implementation of CloseableConnectorContext provides:
/// - Task reconfiguration requests forwarded to the Herder
/// - Error handling forwarded to the Herder's onFailure callback
/// - Plugin metrics for the connector
/// - Close functionality to prevent zombie connector threads
///
/// Corresponds to `org.apache.kafka.connect.runtime.HerderConnectorContext` in Java.
pub struct HerderConnectorContext {
    /// The herder that owns this context.
    herder: Option<Box<dyn HerderCallback>>,
    /// The connector name.
    connector_name: String,
    /// Plugin metrics for this connector.
    plugin_metrics: Option<Box<dyn PluginMetrics>>,
    /// Whether this context has been closed.
    closed: bool,
}

/// Trait for herder callbacks needed by HerderConnectorContext.
///
/// This trait abstracts the herder operations needed by the connector context,
/// allowing for different herder implementations (standalone, distributed).
pub trait HerderCallback: Send + Sync {
    /// Request task reconfiguration for a connector.
    fn request_task_reconfiguration(&self, connector_name: &str);

    /// Handle a connector failure.
    fn on_failure(&self, connector_name: &str, error: &dyn std::error::Error);
}

impl HerderConnectorContext {
    /// Creates a new HerderConnectorContext.
    ///
    /// # Arguments
    /// * `herder` - The herder to forward requests to
    /// * `connector_name` - The name of the connector
    /// * `plugin_metrics` - Plugin metrics for this connector
    ///
    /// Corresponds to Java: `public HerderConnectorContext(AbstractHerder herder, String connectorName, PluginMetricsImpl pluginMetrics)`
    pub fn new(
        herder: Box<dyn HerderCallback>,
        connector_name: String,
        plugin_metrics: Option<Box<dyn PluginMetrics>>,
    ) -> Self {
        HerderConnectorContext {
            herder: Some(herder),
            connector_name,
            plugin_metrics,
            closed: false,
        }
    }

    /// Creates a context without a herder (for testing purposes).
    pub fn new_without_herder(connector_name: String) -> Self {
        HerderConnectorContext {
            herder: None,
            connector_name,
            plugin_metrics: None,
            closed: false,
        }
    }

    /// Check if this context is closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Get the connector name.
    pub fn connector_name(&self) -> &str {
        &self.connector_name
    }

    /// Get the plugin metrics for this connector.
    ///
    /// Returns the plugin metrics instance that was provided when creating
    /// this context. Returns None if no metrics were provided or if the
    /// context has been closed.
    ///
    /// Corresponds to Java: `public PluginMetrics pluginMetrics()`
    pub fn plugin_metrics(&self) -> Option<&dyn PluginMetrics> {
        if self.closed {
            return None;
        }
        self.plugin_metrics.as_ref().map(|m| m.as_ref())
    }

    /// Close the plugin metrics quietly (without throwing errors).
    fn close_plugin_metrics_quietly(&mut self) {
        // In a real implementation, this would close the plugin metrics
        // Similar to Java's Utils.closeQuietly
        self.plugin_metrics = None;
    }
}

impl ConnectorContext for HerderConnectorContext {
    /// Request task reconfiguration for this connector.
    ///
    /// This is forwarded to the herder which will handle the reconfiguration
    /// request (possibly forwarding to the leader in distributed mode).
    ///
    /// Throws ConnectException if the context has been closed.
    ///
    /// Corresponds to Java: `public void requestTaskReconfiguration()`
    fn request_task_reconfiguration(&mut self) {
        if self.closed {
            let msg = format!(
                "The request for task reconfiguration has been rejected because this instance of the connector '{}' has already been shut down.",
                self.connector_name
            );
            warn!("{}", msg);
            panic!("{}", msg);
        }

        // Local herder runs in memory in this process
        // Distributed herder will forward the request to the leader if needed
        if let Some(herder) = &self.herder {
            herder.request_task_reconfiguration(&self.connector_name);
        }
    }

    /// Raise an error for this connector.
    ///
    /// This is forwarded to the herder's onFailure callback which will
    /// mark the connector as failed.
    ///
    /// Throws ConnectException if the context has been closed.
    ///
    /// Corresponds to Java: `public void raiseError(Exception e)`
    fn raise_error(&mut self, error: ConnectError) {
        if self.closed {
            warn!(
                "Connector {} attempted to raise error after shutdown: {:?}",
                self.connector_name, error
            );
            let msg = format!(
                "The request to fail the connector has been rejected because this instance of the connector '{}' has already been shut down.",
                self.connector_name
            );
            panic!("{}", msg);
        }

        if let Some(herder) = &self.herder {
            herder.on_failure(&self.connector_name, &error);
        }
    }
}

impl CloseableConnectorContext for HerderConnectorContext {
    /// Close this connector context.
    ///
    /// After calling this method, all future calls to the context will
    /// throw an exception. This prevents zombie connector threads from
    /// making calls after their connector instance should be shut down.
    ///
    /// Corresponds to Java: `public void close()`
    fn close(&mut self) {
        self.close_plugin_metrics_quietly();
        self.closed = true;
    }
}

impl Drop for HerderConnectorContext {
    fn drop(&mut self) {
        if !self.closed {
            self.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// Mock herder callback for testing.
    struct MockHerderCallback {
        reconfig_requests: Arc<Mutex<Vec<String>>>,
        failures: Arc<Mutex<Vec<(String, String)>>>,
    }

    /// Mock plugin metrics for testing.
    struct MockPluginMetrics {
        prefix: String,
        metrics: Arc<Mutex<Vec<String>>>,
    }

    impl MockPluginMetrics {
        fn new(prefix: &str) -> Self {
            MockPluginMetrics {
                prefix: prefix.to_string(),
                metrics: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl PluginMetrics for MockPluginMetrics {
        fn metric_group_prefix(&self) -> &str {
            &self.prefix
        }

        fn add_metric(&mut self, name: &str) {
            self.metrics.lock().unwrap().push(name.to_string());
        }

        fn remove_metric(&mut self, name: &str) {
            self.metrics.lock().unwrap().retain(|m| m != name);
        }

        fn metric_names(&self) -> Vec<String> {
            self.metrics
                .lock()
                .unwrap()
                .iter()
                .map(|s| s.clone())
                .collect()
        }
    }

    impl MockHerderCallback {
        fn new() -> Self {
            MockHerderCallback {
                reconfig_requests: Arc::new(Mutex::new(Vec::new())),
                failures: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl HerderCallback for MockHerderCallback {
        fn request_task_reconfiguration(&self, connector_name: &str) {
            self.reconfig_requests
                .lock()
                .unwrap()
                .push(connector_name.to_string());
        }

        fn on_failure(&self, connector_name: &str, error: &dyn std::error::Error) {
            self.failures
                .lock()
                .unwrap()
                .push((connector_name.to_string(), error.to_string()));
        }
    }

    #[test]
    fn test_new_context() {
        let herder = Box::new(MockHerderCallback::new());
        let ctx = HerderConnectorContext::new(herder, "test-connector".to_string(), None);

        assert_eq!(ctx.connector_name(), "test-connector");
        assert!(!ctx.is_closed());
    }

    #[test]
    fn test_request_task_reconfiguration() {
        let reconfig_requests = Arc::new(Mutex::new(Vec::new()));
        let herder = Box::new(MockHerderCallback {
            reconfig_requests: Arc::clone(&reconfig_requests),
            failures: Arc::new(Mutex::new(Vec::new())),
        });

        let mut ctx = HerderConnectorContext::new(herder, "test-connector".to_string(), None);

        ctx.request_task_reconfiguration();

        let requests = reconfig_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0], "test-connector");
    }

    #[test]
    fn test_raise_error() {
        let failures = Arc::new(Mutex::new(Vec::new()));
        let herder = Box::new(MockHerderCallback {
            reconfig_requests: Arc::new(Mutex::new(Vec::new())),
            failures: Arc::clone(&failures),
        });

        let mut ctx = HerderConnectorContext::new(herder, "test-connector".to_string(), None);

        ctx.raise_error(ConnectError::general("Test error"));

        let failures = failures.lock().unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].0, "test-connector");
        assert!(failures[0].1.contains("Test error"));
    }

    #[test]
    fn test_close_prevents_reconfiguration() {
        let herder = Box::new(MockHerderCallback::new());
        let mut ctx = HerderConnectorContext::new(herder, "test-connector".to_string(), None);

        ctx.close();
        assert!(ctx.is_closed());

        // This should panic because context is closed
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut ctx2 = HerderConnectorContext::new_without_herder("test".to_string());
            ctx2.close();
            ctx2.request_task_reconfiguration();
        }));

        assert!(result.is_err());
    }

    #[test]
    fn test_close_prevents_raise_error() {
        let herder = Box::new(MockHerderCallback::new());
        let mut ctx = HerderConnectorContext::new(herder, "test-connector".to_string(), None);

        ctx.close();
        assert!(ctx.is_closed());

        // This should panic because context is closed
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut ctx2 = HerderConnectorContext::new_without_herder("test".to_string());
            ctx2.close();
            ctx2.raise_error(ConnectError::general("Test error"));
        }));

        assert!(result.is_err());
    }

    #[test]
    fn test_plugin_metrics_none() {
        let herder = Box::new(MockHerderCallback::new());
        let ctx = HerderConnectorContext::new(herder, "test-connector".to_string(), None);

        // No plugin metrics provided
        assert!(ctx.plugin_metrics().is_none());
    }

    #[test]
    fn test_plugin_metrics_some() {
        let herder = Box::new(MockHerderCallback::new());
        let metrics = Box::new(MockPluginMetrics::new("test-prefix"));
        let ctx = HerderConnectorContext::new(herder, "test-connector".to_string(), Some(metrics));

        // Plugin metrics should be accessible
        let pm = ctx.plugin_metrics();
        assert!(pm.is_some());
        assert_eq!(pm.unwrap().metric_group_prefix(), "test-prefix");
    }

    #[test]
    fn test_plugin_metrics_returns_none_after_close() {
        let herder = Box::new(MockHerderCallback::new());
        let metrics = Box::new(MockPluginMetrics::new("test-prefix"));
        let mut ctx =
            HerderConnectorContext::new(herder, "test-connector".to_string(), Some(metrics));

        // Before close, metrics should be available
        assert!(ctx.plugin_metrics().is_some());

        // After close, metrics should return None
        ctx.close();
        assert!(ctx.plugin_metrics().is_none());
    }
}
