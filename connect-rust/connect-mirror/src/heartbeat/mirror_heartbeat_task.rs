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

//! MirrorHeartbeatTask - Source task for MirrorMaker 2 heartbeat emission.
//!
//! This module provides the MirrorHeartbeatTask class that emits heartbeat records
//! to the target cluster to monitor the health and connectivity of the replication process.
//!
//! The MirrorHeartbeatTask:
//! - Emits heartbeat records containing source and target cluster aliases
//! - Uses a configurable interval for heartbeat emission
//! - Heartbeat records help determine reachability and latency between clusters
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorHeartbeatTask

use std::collections::HashMap;
use std::time::Duration;

use common_trait::protocol::SchemaError;
use connect_api::connector::Task;
use connect_api::errors::ConnectError;
use connect_api::source::{SourceRecord, SourceTask as SourceTaskTrait, SourceTaskContext};
use connect_mirror_client::Heartbeat;
use kafka_clients_mock::RecordMetadata;
use serde_json::{json, Value};

use crate::config::{SOURCE_CLUSTER_ALIAS_CONFIG, SOURCE_CLUSTER_ALIAS_DEFAULT};

use super::mirror_heartbeat_config::{
    MirrorHeartbeatConfig, EMIT_HEARTBEATS_ENABLED_CONFIG, EMIT_HEARTBEATS_ENABLED_DEFAULT,
    EMIT_HEARTBEATS_INTERVAL_SECONDS_CONFIG, EMIT_HEARTBEATS_INTERVAL_SECONDS_DEFAULT,
};

// ============================================================================
// MirrorHeartbeatTask
// ============================================================================

/// Source task for MirrorMaker 2 heartbeat emission.
///
/// The MirrorHeartbeatTask is responsible for:
/// - Emitting heartbeat records to the target cluster
/// - Monitoring the health and connectivity of the replication process
///
/// Heartbeat records contain:
/// - Source cluster alias
/// - Target cluster alias
/// - Timestamp
///
/// These records help determine reachability and latency between clusters.
/// The task emits heartbeats at a configurable interval.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.MirrorHeartbeatTask
pub struct MirrorHeartbeatTask {
    /// Task context
    context: Option<Box<dyn SourceTaskContext>>,
    /// MirrorHeartbeatConfig configuration
    config: Option<MirrorHeartbeatConfig>,
    /// Source cluster alias
    source_cluster_alias: String,
    /// Target cluster alias
    target_cluster_alias: String,
    /// Heartbeats topic name
    heartbeats_topic: String,
    /// Emit interval for heartbeats
    emit_interval: Duration,
    /// Flag indicating task is stopping
    stopping: bool,
    /// Flag indicating task has been initialized
    initialized: bool,
    /// Last emit time (for interval-based emission)
    last_emit_time: Option<std::time::Instant>,
}

impl std::fmt::Debug for MirrorHeartbeatTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorHeartbeatTask")
            .field("source_cluster_alias", &self.source_cluster_alias)
            .field("target_cluster_alias", &self.target_cluster_alias)
            .field("heartbeats_topic", &self.heartbeats_topic)
            .field("emit_interval", &self.emit_interval)
            .field("stopping", &self.stopping)
            .field("initialized", &self.initialized)
            .finish()
    }
}

impl MirrorHeartbeatTask {
    /// Creates a new MirrorHeartbeatTask.
    pub fn new() -> Self {
        MirrorHeartbeatTask {
            context: None,
            config: None,
            source_cluster_alias: SOURCE_CLUSTER_ALIAS_DEFAULT.to_string(),
            target_cluster_alias: crate::config::TARGET_CLUSTER_ALIAS_DEFAULT.to_string(),
            heartbeats_topic: String::new(),
            emit_interval: Duration::from_secs(EMIT_HEARTBEATS_INTERVAL_SECONDS_DEFAULT as u64),
            stopping: false,
            initialized: false,
            last_emit_time: None,
        }
    }

    /// Returns the source cluster alias.
    pub fn source_cluster_alias(&self) -> &str {
        &self.source_cluster_alias
    }

    /// Returns the target cluster alias.
    pub fn target_cluster_alias(&self) -> &str {
        &self.target_cluster_alias
    }

    /// Returns the heartbeats topic name.
    pub fn heartbeats_topic(&self) -> &str {
        &self.heartbeats_topic
    }

    /// Returns the emit interval.
    pub fn emit_interval(&self) -> Duration {
        self.emit_interval
    }

    /// Creates a SourceRecord from a Heartbeat.
    ///
    /// The SourceRecord is sent to the heartbeats topic.
    ///
    /// Corresponds to Java: MirrorHeartbeatTask.heartbeatRecord()
    fn heartbeat_record(&self, heartbeat: &Heartbeat) -> Result<SourceRecord, SchemaError> {
        // Build source partition from heartbeat's connect partition
        let connect_partition = heartbeat.connect_partition();
        let source_partition: HashMap<String, Value> = connect_partition
            .into_iter()
            .map(|(k, v)| (k, json!(v)))
            .collect();

        // Build source offset - we use timestamp as the offset
        let source_offset: HashMap<String, Value> =
            HashMap::from([("timestamp".to_string(), json!(heartbeat.timestamp()))]);

        // Serialize heartbeat key and value
        let key_bytes = heartbeat.record_key()?;
        let value_bytes = heartbeat.record_value()?;

        // Create SourceRecord
        // Key is the serialized heartbeat key (sourceClusterAlias + targetClusterAlias)
        // Value is the serialized heartbeat value (timestamp)
        let key = Some(json!(key_bytes));
        let value = json!(value_bytes);

        Ok(SourceRecord::new(
            source_partition,
            source_offset,
            self.heartbeats_topic.clone(),
            Some(0), // Heartbeats topic has single partition
            key,
            value,
        ))
    }

    /// Creates a Heartbeat with current timestamp.
    fn create_heartbeat(&self) -> Heartbeat {
        Heartbeat::new(
            self.source_cluster_alias.clone(),
            self.target_cluster_alias.clone(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        )
    }
}

impl Default for MirrorHeartbeatTask {
    fn default() -> Self {
        Self::new()
    }
}

impl Task for MirrorHeartbeatTask {
    fn version(&self) -> &str {
        "1.0.0"
    }

    /// Starts the task with the given configuration.
    ///
    /// This initializes:
    /// - Source and target cluster aliases
    /// - Heartbeats topic name
    /// - Emit interval
    ///
    /// Corresponds to Java: MirrorHeartbeatTask.start(Map<String, String> props)
    fn start(&mut self, props: HashMap<String, String>) {
        // Create configuration from properties
        self.config = Some(MirrorHeartbeatConfig::new(props.clone()));

        let config = self.config.as_ref().unwrap();

        // Set source and target cluster aliases
        self.source_cluster_alias = props
            .get(SOURCE_CLUSTER_ALIAS_CONFIG)
            .map(|s| s.clone())
            .unwrap_or_else(|| config.source_cluster_alias().to_string());

        self.target_cluster_alias = props
            .get(crate::config::TARGET_CLUSTER_ALIAS_CONFIG)
            .map(|s| s.clone())
            .unwrap_or_else(|| config.target_cluster_alias().to_string());

        // Set heartbeats topic
        self.heartbeats_topic = config.heartbeats_topic();

        // Set emit interval
        self.emit_interval = config.emit_heartbeats_interval();

        // Check if heartbeat emission is enabled
        let enabled = props
            .get(EMIT_HEARTBEATS_ENABLED_CONFIG)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(EMIT_HEARTBEATS_ENABLED_DEFAULT);

        if !enabled {
            // If disabled, set interval to zero
            self.emit_interval = Duration::ZERO;
        }

        // Reset stopping flag
        self.stopping = false;

        // Set initialized flag
        self.initialized = true;

        // Set initial emit time
        self.last_emit_time = Some(std::time::Instant::now());
    }

    /// Stops the task.
    ///
    /// This clears internal state.
    ///
    /// Corresponds to Java: MirrorHeartbeatTask.stop()
    fn stop(&mut self) {
        // Set stopping flag
        self.stopping = true;

        // Clear internal state
        self.initialized = false;
        self.last_emit_time = None;
    }
}

impl SourceTaskTrait for MirrorHeartbeatTask {
    /// Initializes this source task with the specified context.
    ///
    /// The context provides access to the offset storage reader
    /// for loading previously committed offsets.
    ///
    /// Corresponds to Java: SourceTask.initialize(SourceTaskContext context)
    fn initialize(&mut self, context: Box<dyn SourceTaskContext>) {
        self.context = Some(context);
    }

    /// Polls for new heartbeat record to emit.
    ///
    /// This creates a heartbeat record if the emit interval has elapsed.
    /// The poll() method respects the emit_heartbeats_interval configuration.
    /// If the interval has not elapsed since the last emission, returns empty.
    ///
    /// Corresponds to Java: MirrorHeartbeatTask.poll()
    fn poll(&mut self) -> Result<Vec<SourceRecord>, ConnectError> {
        // Check if stopping
        if self.stopping {
            return Ok(Vec::new());
        }

        // Check if initialized
        if !self.initialized {
            return Ok(Vec::new());
        }

        // Check if emit interval is zero (disabled)
        if self.emit_interval.is_zero() {
            return Ok(Vec::new());
        }

        // Check emit interval
        if let Some(last_emit) = self.last_emit_time {
            if std::time::Instant::now().duration_since(last_emit) < self.emit_interval {
                // Interval not elapsed, return empty
                return Ok(Vec::new());
            }
        }

        // Create heartbeat
        let heartbeat = self.create_heartbeat();

        // Create SourceRecord from heartbeat
        let record = self
            .heartbeat_record(&heartbeat)
            .map_err(|e| ConnectError::data(format!("Failed to create heartbeat record: {}", e)))?;

        // Update last emit time
        self.last_emit_time = Some(std::time::Instant::now());

        Ok(vec![record])
    }

    /// Commits the current batch of records.
    ///
    /// For MirrorHeartbeatTask, this is a no-operation.
    /// Heartbeat records are emitted directly to the heartbeats topic.
    ///
    /// Corresponds to Java: MirrorHeartbeatTask.commit()
    fn commit(&mut self) -> Result<(), ConnectError> {
        // No-op in Java implementation
        Ok(())
    }

    /// Commits a single record after it has been successfully sent.
    ///
    /// For MirrorHeartbeatTask, this is a no-operation.
    ///
    /// Corresponds to Java: MirrorHeartbeatTask.commitRecord(SourceRecord, RecordMetadata)
    fn commit_record(
        &mut self,
        _record: &SourceRecord,
        _metadata: &RecordMetadata,
    ) -> Result<(), ConnectError> {
        // No-op in Java implementation
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use common_trait::metrics::PluginMetrics;
    use connect_api::storage::OffsetStorageReader;
    use connect_api::ConnectRecord;

    /// Mock OffsetStorageReader for testing
    struct MockOffsetStorageReader {
        offsets: Vec<(HashMap<String, Value>, HashMap<String, Value>)>,
    }

    impl MockOffsetStorageReader {
        fn new() -> Self {
            MockOffsetStorageReader {
                offsets: Vec::new(),
            }
        }
    }

    impl OffsetStorageReader for MockOffsetStorageReader {
        fn offset(&self, partition: &HashMap<String, Value>) -> Option<HashMap<String, Value>> {
            self.offsets
                .iter()
                .find(|(key, _)| key == partition)
                .map(|(_, value)| value.clone())
        }
    }

    /// Mock SourceTaskContext for testing
    struct MockSourceTaskContext {
        offset_reader: MockOffsetStorageReader,
    }

    impl MockSourceTaskContext {
        fn new() -> Self {
            MockSourceTaskContext {
                offset_reader: MockOffsetStorageReader::new(),
            }
        }
    }

    impl SourceTaskContext for MockSourceTaskContext {
        fn offset_storage_reader(&self) -> &dyn OffsetStorageReader {
            &self.offset_reader
        }

        fn plugin_metrics(&self) -> Option<&(dyn PluginMetrics + Send + Sync)> {
            None
        }
    }

    fn create_test_config() -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert(
            SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
            "backup".to_string(),
        );
        props.insert(
            crate::config::TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
            "primary".to_string(),
        );
        props.insert(
            crate::config::NAME_CONFIG.to_string(),
            "mirror-heartbeat-task".to_string(),
        );
        props
    }

    #[test]
    fn test_new_task() {
        let task = MirrorHeartbeatTask::new();
        assert_eq!(task.source_cluster_alias(), SOURCE_CLUSTER_ALIAS_DEFAULT);
        assert_eq!(
            task.target_cluster_alias(),
            crate::config::TARGET_CLUSTER_ALIAS_DEFAULT
        );
        assert!(task.heartbeats_topic().is_empty());
        assert!(!task.stopping);
        assert!(!task.initialized);
    }

    #[test]
    fn test_version() {
        let task = MirrorHeartbeatTask::new();
        assert_eq!(task.version(), "1.0.0");
    }

    #[test]
    fn test_start_with_config() {
        let mut task = MirrorHeartbeatTask::new();
        let props = create_test_config();

        task.start(props);

        assert_eq!(task.source_cluster_alias(), "backup");
        assert_eq!(task.target_cluster_alias(), "primary");
        assert_eq!(task.heartbeats_topic(), "backup.heartbeats");
        assert_eq!(task.emit_interval(), Duration::from_secs(1));
        assert!(!task.stopping);
        assert!(task.initialized);
    }

    #[test]
    fn test_start_with_custom_interval() {
        let mut task = MirrorHeartbeatTask::new();
        let mut props = create_test_config();
        props.insert(
            EMIT_HEARTBEATS_INTERVAL_SECONDS_CONFIG.to_string(),
            "5".to_string(),
        );

        task.start(props);

        assert_eq!(task.emit_interval(), Duration::from_secs(5));
    }

    #[test]
    fn test_start_disabled() {
        let mut task = MirrorHeartbeatTask::new();
        let mut props = create_test_config();
        props.insert(
            EMIT_HEARTBEATS_ENABLED_CONFIG.to_string(),
            "false".to_string(),
        );

        task.start(props);

        assert_eq!(task.emit_interval(), Duration::ZERO);
    }

    #[test]
    fn test_stop() {
        let mut task = MirrorHeartbeatTask::new();
        let props = create_test_config();
        task.start(props);

        task.stop();

        assert!(task.stopping);
        assert!(!task.initialized);
        assert!(task.last_emit_time.is_none());
    }

    #[test]
    fn test_poll_produces_heartbeat() {
        let mut task = MirrorHeartbeatTask::new();
        let props = create_test_config();
        task.start(props);

        // Set last emit time to 2 seconds ago so interval elapsed
        task.last_emit_time = Some(std::time::Instant::now() - Duration::from_secs(2));

        // Poll should return a heartbeat record
        let records = task.poll().unwrap();
        assert_eq!(records.len(), 1);

        // Verify heartbeat record structure
        let record = &records[0];
        assert_eq!(record.topic(), task.heartbeats_topic());
        assert_eq!(record.kafka_partition(), Some(0));

        // Source partition should contain source and target cluster aliases
        let source_partition = record.source_partition();
        assert_eq!(
            source_partition.get("sourceClusterAlias"),
            Some(&json!("backup"))
        );
        assert_eq!(
            source_partition.get("targetClusterAlias"),
            Some(&json!("primary"))
        );

        // Source offset should contain timestamp
        let source_offset = record.source_offset();
        assert!(source_offset.contains_key("timestamp"));
    }

    #[test]
    fn test_poll_when_stopping() {
        let mut task = MirrorHeartbeatTask::new();
        let props = create_test_config();
        task.start(props);

        task.stopping = true;

        let records = task.poll().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_poll_when_not_initialized() {
        let mut task = MirrorHeartbeatTask::new();

        // Not initialized - poll returns empty
        let records = task.poll().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_poll_interval_respected() {
        let mut task = MirrorHeartbeatTask::new();
        let props = create_test_config();
        task.start(props);

        // Set emit interval to 10 seconds
        task.emit_interval = Duration::from_secs(10);
        task.last_emit_time = Some(std::time::Instant::now());

        // Poll immediately - should return empty because interval not elapsed
        let records = task.poll().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_poll_interval_elapsed() {
        let mut task = MirrorHeartbeatTask::new();
        let props = create_test_config();
        task.start(props);

        // Set emit interval to 1 second
        task.emit_interval = Duration::from_secs(1);

        // Set last emit time to 2 seconds ago (interval elapsed)
        task.last_emit_time = Some(std::time::Instant::now() - Duration::from_secs(2));

        // Poll should return heartbeat record
        let records = task.poll().unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn test_commit() {
        let mut task = MirrorHeartbeatTask::new();
        let result = task.commit();
        assert!(result.is_ok());
    }

    #[test]
    fn test_commit_record() {
        let mut task = MirrorHeartbeatTask::new();

        let source_partition = HashMap::from([
            ("sourceClusterAlias".to_string(), json!("backup")),
            ("targetClusterAlias".to_string(), json!("primary")),
        ]);
        let source_offset = HashMap::from([("timestamp".to_string(), json!(1234567890))]);

        let record = SourceRecord::new(
            source_partition,
            source_offset,
            "backup.heartbeats",
            Some(0),
            Some(json!(vec![1, 2, 3])),
            json!(vec![4, 5, 6]),
        );

        let tp = common_trait::TopicPartition::new("backup.heartbeats", 0);
        let metadata = RecordMetadata::new(tp, 0, 0, 12345, 1, 2);

        let result = task.commit_record(&record, &metadata);
        assert!(result.is_ok());
    }

    #[test]
    fn test_initialize() {
        let mut task = MirrorHeartbeatTask::new();
        let context = Box::new(MockSourceTaskContext::new());

        task.initialize(context);

        assert!(task.context.is_some());
    }

    #[test]
    fn test_create_heartbeat() {
        let mut task = MirrorHeartbeatTask::new();
        let props = create_test_config();
        task.start(props);

        let heartbeat = task.create_heartbeat();

        assert_eq!(heartbeat.source_cluster_alias(), "backup");
        assert_eq!(heartbeat.target_cluster_alias(), "primary");
        assert!(heartbeat.timestamp() > 0);
    }

    #[test]
    fn test_heartbeat_record() {
        let mut task = MirrorHeartbeatTask::new();
        let props = create_test_config();
        task.start(props);

        let heartbeat = Heartbeat::new("backup", "primary", 1234567890);
        let record = task.heartbeat_record(&heartbeat).unwrap();

        // Verify record structure
        assert_eq!(record.topic(), task.heartbeats_topic());
        assert_eq!(record.kafka_partition(), Some(0));

        // Source partition should contain source and target cluster aliases
        let source_partition = record.source_partition();
        assert_eq!(
            source_partition.get("sourceClusterAlias"),
            Some(&json!("backup"))
        );
        assert_eq!(
            source_partition.get("targetClusterAlias"),
            Some(&json!("primary"))
        );

        // Source offset should contain timestamp
        let source_offset = record.source_offset();
        assert_eq!(source_offset.get("timestamp"), Some(&json!(1234567890)));
    }

    #[test]
    fn test_missing_source_alias_config() {
        let mut task = MirrorHeartbeatTask::new();
        let mut props = HashMap::new();
        props.insert(
            crate::config::TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
            "primary".to_string(),
        );

        task.start(props);

        // Should use default value
        assert_eq!(task.source_cluster_alias(), SOURCE_CLUSTER_ALIAS_DEFAULT);
    }

    #[test]
    fn test_missing_target_alias_config() {
        let mut task = MirrorHeartbeatTask::new();
        let mut props = HashMap::new();
        props.insert(
            SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
            "backup".to_string(),
        );

        task.start(props);

        // Should use default value
        assert_eq!(
            task.target_cluster_alias(),
            crate::config::TARGET_CLUSTER_ALIAS_DEFAULT
        );
    }

    #[test]
    fn test_multiple_poll_calls() {
        let mut task = MirrorHeartbeatTask::new();
        let props = create_test_config();
        task.start(props);

        // Set emit interval to a very short duration for testing
        task.emit_interval = Duration::from_millis(1);

        // First poll - set last emit time to past
        task.last_emit_time = Some(std::time::Instant::now() - Duration::from_secs(2));
        let records1 = task.poll().unwrap();
        assert_eq!(records1.len(), 1);

        // Wait a bit for second poll
        std::thread::sleep(Duration::from_millis(5));

        // Second poll
        let records2 = task.poll().unwrap();
        assert_eq!(records2.len(), 1);

        // Verify timestamps are different (updated on each poll)
        let ts1 = records1[0]
            .source_offset()
            .get("timestamp")
            .and_then(|v| v.as_i64())
            .unwrap();
        let ts2 = records2[0]
            .source_offset()
            .get("timestamp")
            .and_then(|v| v.as_i64())
            .unwrap();

        // Timestamps should be different (or at least second >= first)
        assert!(ts2 >= ts1);
    }
}
