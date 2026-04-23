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

//! WorkerSinkTaskContext implementation.
//!
//! Provides the context for WorkerSinkTask to interact with the consumer
//! and manage offsets, partitions, and commits.
//!
//! Corresponds to `org.apache.kafka.connect.runtime.WorkerSinkTaskContext` in Java.

use common_trait::metrics::PluginMetrics;
use common_trait::storage::ConnectorTaskId;
use common_trait::TopicPartition;
use connect_api::ErrantRecordReporter;
use connect_api::SinkTaskContext;
use kafka_clients_mock::OffsetAndMetadata;
use log::debug;
use std::collections::{HashMap, HashSet};

/// WorkerSinkTaskContext provides the context for WorkerSinkTask.
///
/// This implementation of SinkTaskContext provides:
/// - Offset management for sink records
/// - Timeout configuration
/// - Partition assignment tracking
/// - Pause/resume partition management
/// - Commit request handling
/// - Access to errant record reporter
///
/// Corresponds to `org.apache.kafka.connect.runtime.WorkerSinkTaskContext` in Java.
pub struct WorkerSinkTaskContext {
    /// Offsets that have been requested to be reset.
    offsets: HashMap<TopicPartition, i64>,
    /// Reference to the consumer (mocked as topic partition set).
    consumer_assignment: HashSet<TopicPartition>,
    /// Task ID for this sink task.
    task_id: ConnectorTaskId,
    /// Configuration state for the task.
    config_state: HashMap<String, String>,
    /// Partitions that have been explicitly paused by the connector.
    paused_partitions: HashSet<TopicPartition>,
    /// Current timeout in milliseconds.
    timeout_ms: i64,
    /// Whether a commit has been requested.
    commit_requested: bool,
    /// Errant record reporter for DLQ handling.
    errant_record_reporter: Option<Box<dyn ErrantRecordReporter>>,
    /// Plugin metrics for this task.
    plugin_metrics: Option<Box<dyn PluginMetrics + Send + Sync>>,
    /// Whether the connector should pause.
    should_pause: bool,
}

impl WorkerSinkTaskContext {
    /// Creates a new WorkerSinkTaskContext.
    ///
    /// # Arguments
    /// * `consumer_assignment` - Initial consumer assignment
    /// * `task_id` - The ConnectorTaskId for this task
    /// * `config_state` - Configuration state for the task
    ///
    /// Corresponds to Java: `public WorkerSinkTaskContext(Consumer<byte[], byte[]> consumer, WorkerSinkTask sinkTask, ClusterConfigState configState)`
    pub fn new(
        consumer_assignment: HashSet<TopicPartition>,
        task_id: ConnectorTaskId,
        config_state: HashMap<String, String>,
    ) -> Self {
        WorkerSinkTaskContext {
            offsets: HashMap::new(),
            consumer_assignment,
            task_id,
            config_state,
            paused_partitions: HashSet::new(),
            timeout_ms: -1,
            commit_requested: false,
            errant_record_reporter: None,
            plugin_metrics: None,
            should_pause: false,
        }
    }

    /// Creates a minimal context for testing.
    pub fn new_for_testing(task_id: ConnectorTaskId) -> Self {
        WorkerSinkTaskContext {
            offsets: HashMap::new(),
            consumer_assignment: HashSet::new(),
            task_id,
            config_state: HashMap::new(),
            paused_partitions: HashSet::new(),
            timeout_ms: -1,
            commit_requested: false,
            errant_record_reporter: None,
            plugin_metrics: None,
            should_pause: false,
        }
    }

    /// Clear all offsets.
    pub fn clear_offsets(&mut self) {
        self.offsets.clear();
    }

    /// Get the offsets that have been submitted to be reset.
    ///
    /// Used by the Kafka Connect framework.
    pub fn offsets(&self) -> &HashMap<TopicPartition, i64> {
        &self.offsets
    }

    /// Get the timeout in milliseconds.
    ///
    /// Used by the Kafka Connect framework.
    pub fn timeout(&self) -> i64 {
        self.timeout_ms
    }

    /// Get the paused partitions.
    pub fn paused_partitions(&self) -> &HashSet<TopicPartition> {
        &self.paused_partitions
    }

    /// Check if a commit has been requested.
    pub fn is_commit_requested(&self) -> bool {
        self.commit_requested
    }

    /// Clear the commit request flag.
    pub fn clear_commit_request(&mut self) {
        self.commit_requested = false;
    }

    /// Get the task ID.
    pub fn task_id(&self) -> &ConnectorTaskId {
        &self.task_id
    }

    /// Set whether the connector should pause.
    pub fn set_should_pause(&mut self, should_pause: bool) {
        self.should_pause = should_pause;
    }

    /// Check if the connector should pause.
    pub fn should_pause(&self) -> bool {
        self.should_pause
    }

    /// Set the errant record reporter.
    pub fn set_errant_record_reporter(&mut self, reporter: Box<dyn ErrantRecordReporter>) {
        self.errant_record_reporter = Some(reporter);
    }

    /// Get the errant record reporter.
    pub fn errant_record_reporter(&self) -> Option<&dyn ErrantRecordReporter> {
        self.errant_record_reporter.as_ref().map(|r| r.as_ref())
    }

    /// Set the plugin metrics.
    pub fn set_plugin_metrics(&mut self, metrics: Box<dyn PluginMetrics + Send + Sync>) {
        self.plugin_metrics = Some(metrics);
    }

    /// Update consumer assignment.
    pub fn update_assignment(&mut self, assignment: HashSet<TopicPartition>) {
        self.consumer_assignment = assignment;
        // Remove paused partitions that are no longer assigned
        self.paused_partitions
            .retain(|tp| self.consumer_assignment.contains(tp));
    }

    /// Get current consumer assignment reference.
    pub fn assignment_ref(&self) -> &HashSet<TopicPartition> {
        &self.consumer_assignment
    }
}

impl SinkTaskContext for WorkerSinkTaskContext {
    /// Get the offset for the given topic partition.
    fn offset(&self, topic_partition: &TopicPartition) -> Option<&OffsetAndMetadata> {
        // Convert internal i64 offset to OffsetAndMetadata reference
        // This is a simplified implementation - in reality we'd store OffsetAndMetadata
        self.offsets.get(topic_partition).and_then(|_| {
            // Return None for now as we don't have OffsetAndMetadata stored
            None
        })
    }

    /// Request a timeout for the next batch.
    fn request_timeout(&mut self, timeout_ms: i64) {
        debug!("{} Setting timeout to {} ms", self, timeout_ms);
        self.timeout_ms = timeout_ms;
    }

    /// Get the current timeout.
    fn timeout(&self) -> i64 {
        self.timeout_ms
    }

    /// Get the plugin metrics.
    fn plugin_metrics(&self) -> Option<&(dyn PluginMetrics + Send + Sync)> {
        self.plugin_metrics.as_ref().map(|m| m.as_ref())
    }
}

// Additional methods not in trait but needed for WorkerSinkTask
impl WorkerSinkTaskContext {
    /// Set offset for a single topic partition.
    pub fn set_offset(&mut self, tp: TopicPartition, offset: i64) {
        debug!(
            "{} Setting offset for topic partition {} to {}",
            self, tp, offset
        );
        self.offsets.insert(tp, offset);
    }

    /// Set offsets for multiple topic partitions.
    pub fn set_offsets(&mut self, offsets: HashMap<TopicPartition, i64>) {
        debug!(
            "{} Setting offsets for topic partitions {:?}",
            self, offsets
        );
        self.offsets.extend(offsets);
    }

    /// Get the current assignment.
    pub fn assignment(&self) -> HashSet<TopicPartition> {
        if self.consumer_assignment.is_empty() {
            panic!("SinkTaskContext may not be used to look up partition assignment until the task is initialized");
        }
        self.consumer_assignment.clone()
    }

    /// Pause consumption from specified partitions.
    pub fn pause(&mut self, partitions: Vec<TopicPartition>) {
        for partition in &partitions {
            if !self.consumer_assignment.contains(partition) {
                panic!(
                    "SinkTasks may not pause partitions that are not currently assigned to them."
                );
            }
        }

        for partition in partitions {
            self.paused_partitions.insert(partition.clone());
            if self.should_pause {
                debug!(
                    "{} Connector is paused, so not pausing consumer's partition {}",
                    self, partition
                );
            } else {
                debug!(
                    "{} Pausing partition {}. Connector is not paused.",
                    self, partition
                );
            }
        }
    }

    /// Resume consumption from specified partitions.
    pub fn resume(&mut self, partitions: Vec<TopicPartition>) {
        for partition in partitions {
            self.paused_partitions.remove(&partition);
            if self.should_pause {
                debug!(
                    "{} Connector is paused, so not resuming consumer's partition {}",
                    self, partition
                );
            } else {
                debug!("{} Resuming partition: {}", self, partition);
            }
        }
    }

    /// Request a commit.
    pub fn request_commit(&mut self) {
        debug!("{} Requesting commit", self);
        self.commit_requested = true;
    }

    /// Get the errant record reporter for DLQ handling.
    pub fn errant_record_reporter_ref(&self) -> Option<&dyn ErrantRecordReporter> {
        self.errant_record_reporter.as_ref().map(|r| r.as_ref())
    }
}

impl std::fmt::Display for WorkerSinkTaskContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkerSinkTaskContext{{id={}}}", self.task_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_context() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let ctx = WorkerSinkTaskContext::new_for_testing(task_id.clone());

        assert_eq!(ctx.task_id(), &task_id);
        assert!(ctx.offsets().is_empty());
        assert_eq!(ctx.timeout(), -1);
    }

    #[test]
    fn test_set_offset() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut ctx = WorkerSinkTaskContext::new_for_testing(task_id);

        let tp = TopicPartition::new("test-topic".to_string(), 0);
        ctx.set_offset(tp.clone(), 100);

        assert_eq!(ctx.offsets().get(&tp), Some(&100));
    }

    #[test]
    fn test_set_offsets() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut ctx = WorkerSinkTaskContext::new_for_testing(task_id);

        let offsets = HashMap::from([
            (TopicPartition::new("topic1".to_string(), 0), 100),
            (TopicPartition::new("topic1".to_string(), 1), 200),
        ]);
        ctx.set_offsets(offsets);

        assert_eq!(ctx.offsets().len(), 2);
    }

    #[test]
    fn test_clear_offsets() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut ctx = WorkerSinkTaskContext::new_for_testing(task_id);

        ctx.set_offset(TopicPartition::new("test-topic".to_string(), 0), 100);
        ctx.clear_offsets();

        assert!(ctx.offsets().is_empty());
    }

    #[test]
    fn test_timeout() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut ctx = WorkerSinkTaskContext::new_for_testing(task_id);

        ctx.request_timeout(5000);
        assert_eq!(ctx.timeout(), 5000);
    }

    #[test]
    fn test_commit_request() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut ctx = WorkerSinkTaskContext::new_for_testing(task_id);

        assert!(!ctx.is_commit_requested());

        ctx.request_commit();
        assert!(ctx.is_commit_requested());

        ctx.clear_commit_request();
        assert!(!ctx.is_commit_requested());
    }

    #[test]
    fn test_pause_resume() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let tp1 = TopicPartition::new("topic1".to_string(), 0);
        let tp2 = TopicPartition::new("topic1".to_string(), 1);

        let assignment = HashSet::from([tp1.clone(), tp2.clone()]);
        let mut ctx = WorkerSinkTaskContext::new(assignment, task_id, HashMap::new());

        ctx.pause(vec![tp1.clone()]);
        assert!(ctx.paused_partitions().contains(&tp1));
        assert!(!ctx.paused_partitions().contains(&tp2));

        ctx.resume(vec![tp1.clone()]);
        assert!(!ctx.paused_partitions().contains(&tp1));
    }

    #[test]
    fn test_should_pause() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let mut ctx = WorkerSinkTaskContext::new_for_testing(task_id);

        assert!(!ctx.should_pause());

        ctx.set_should_pause(true);
        assert!(ctx.should_pause());
    }

    #[test]
    fn test_display() {
        let task_id = ConnectorTaskId::new("test-connector".to_string(), 0);
        let ctx = WorkerSinkTaskContext::new_for_testing(task_id);

        let display = format!("{}", ctx);
        assert!(display.contains("test-connector"));
        assert!(display.contains("0"));
    }
}
