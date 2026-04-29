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

//! WorkerSourceTaskContext implementation.
//!
//! Provides the context for WorkerSourceTask to interact with offset storage
//! and retrieve configuration.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.WorkerSourceTaskContext` in Java.

use crate::runtime::worker_transaction_context::WorkerTransactionContext;
use common_trait::metrics::PluginMetrics;
use common_trait::storage::ConnectorTaskId;
use connect_api::storage::OffsetStorageReader;
use connect_api::SourceTaskContext;
use std::collections::HashMap;

/// WorkerSourceTaskContext provides the context for WorkerSourceTask.
///
/// This implementation of SourceTaskContext provides:
/// - Access to the offset storage reader for storing/retrieving offsets
/// - Access to the task configuration
/// - Access to the transaction context for exactly-once semantics
/// - Access to plugin metrics
///
/// Corresponds to `org.apache.kafka.connect.runtime.WorkerSourceTaskContext` in Java.
pub struct WorkerSourceTaskContext {
    /// Offset storage reader for reading previously committed offsets.
    offset_storage_reader: Option<Box<dyn OffsetStorageReader + Send + Sync>>,
    /// The task ID for this source task.
    task_id: ConnectorTaskId,
    /// Configuration state for the cluster.
    config_state: Option<HashMap<String, String>>,
    /// Transaction context for exactly-once semantics.
    transaction_context: Option<WorkerTransactionContext>,
    /// Plugin metrics for this task.
    plugin_metrics: Option<Box<dyn PluginMetrics + Send + Sync>>,
}

impl WorkerSourceTaskContext {
    /// Creates a new WorkerSourceTaskContext.
    ///
    /// # Arguments
    /// * `reader` - Offset storage reader for reading previously committed offsets
    /// * `id` - The ConnectorTaskId for this task
    /// * `config_state` - Configuration state for the cluster
    /// * `transaction_context` - Transaction context for exactly-once semantics
    /// * `plugin_metrics` - Plugin metrics for this task
    ///
    /// Corresponds to Java: `public WorkerSourceTaskContext(OffsetStorageReader reader, ConnectorTaskId id, ClusterConfigState configState, WorkerTransactionContext transactionContext, PluginMetrics pluginMetrics)`
    pub fn new(
        offset_storage_reader: Option<Box<dyn OffsetStorageReader + Send + Sync>>,
        task_id: ConnectorTaskId,
        config_state: Option<HashMap<String, String>>,
        transaction_context: Option<WorkerTransactionContext>,
        plugin_metrics: Option<Box<dyn PluginMetrics + Send + Sync>>,
    ) -> Self {
        WorkerSourceTaskContext {
            offset_storage_reader,
            task_id,
            config_state,
            transaction_context,
            plugin_metrics,
        }
    }

    /// Creates a minimal context for testing.
    pub fn new_for_testing(task_id: ConnectorTaskId) -> Self {
        WorkerSourceTaskContext {
            offset_storage_reader: None,
            task_id,
            config_state: None,
            transaction_context: None,
            plugin_metrics: None,
        }
    }

    /// Get the task ID.
    pub fn task_id(&self) -> &ConnectorTaskId {
        &self.task_id
    }

    /// Get the transaction context.
    ///
    /// Returns the transaction context for exactly-once semantics support.
    /// This is only available for tasks that support exactly-once delivery.
    pub fn transaction_context(&self) -> Option<&WorkerTransactionContext> {
        self.transaction_context.as_ref()
    }

    /// Get mutable access to the transaction context.
    pub fn transaction_context_mut(&mut self) -> Option<&mut WorkerTransactionContext> {
        self.transaction_context.as_mut()
    }
}

impl SourceTaskContext for WorkerSourceTaskContext {
    /// Get the offset storage reader.
    ///
    /// Returns a reference to the offset storage reader.
    /// Panics if no reader was provided during construction.
    ///
    /// Corresponds to Java: `public OffsetStorageReader offsetStorageReader()`
    fn offset_storage_reader(&self) -> &dyn OffsetStorageReader {
        self.offset_storage_reader
            .as_ref()
            .map(|r| r.as_ref())
            .expect("offset_storage_reader is required but was not provided")
    }

    /// Get the plugin metrics.
    fn plugin_metrics(&self) -> Option<&(dyn PluginMetrics + Send + Sync)> {
        self.plugin_metrics.as_ref().map(|m| m.as_ref())
    }
}

// Additional methods not in trait
impl WorkerSourceTaskContext {
    /// Get the configuration for this task.
    pub fn configs(&self) -> HashMap<String, String> {
        self.config_state.clone().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_context() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let ctx = WorkerSourceTaskContext::new_for_testing(task_id.clone());

        assert_eq!(ctx.task_id(), &task_id);
        assert!(ctx.configs().is_empty());
    }

    #[test]
    fn test_context_with_config() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let config = HashMap::from([("key".to_string(), "value".to_string())]);

        let ctx = WorkerSourceTaskContext::new(None, task_id, Some(config), None, None);

        let configs = ctx.configs();
        assert_eq!(configs.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_context_with_transaction_context() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let transaction_ctx = WorkerTransactionContext::new();

        let ctx = WorkerSourceTaskContext::new(None, task_id, None, Some(transaction_ctx), None);

        assert!(ctx.transaction_context().is_some());
    }
}
