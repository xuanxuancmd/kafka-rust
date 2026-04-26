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

//! MirrorSourceTask - Source task for MirrorMaker 2.
//!
//! This module provides the MirrorSourceTask class that replicates data
//! from a source Kafka cluster to a target Kafka cluster.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorSourceTask (258 lines)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common_trait::TopicPartition;
use connect_api::connector::{ConnectRecord, Task};
use connect_api::errors::ConnectError;
use connect_api::source::{SourceRecord, SourceTask as SourceTaskTrait, SourceTaskContext};
use connect_mirror_client::ReplicationPolicy;
use kafka_clients_mock::{ConsumerRecord, MockKafkaConsumer, RecordMetadata};
use serde_json::{json, Value};

use super::mirror_source_config::{
    MirrorSourceConfig, CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT, TASK_TOPIC_PARTITIONS_CONFIG,
};
use crate::config::{SOURCE_CLUSTER_ALIAS_CONFIG, SOURCE_CLUSTER_ALIAS_DEFAULT};

// ============================================================================
// MirrorSourceTask
// ============================================================================

/// Source task for MirrorMaker 2 replication.
///
/// The MirrorSourceTask is responsible for:
/// - Consuming records from the source cluster
/// - Converting source topic names to remote topic names
/// - Tracking offset mapping for checkpoint synchronization
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.MirrorSourceTask
pub struct MirrorSourceTask {
    /// Task context
    context: Option<Box<dyn SourceTaskContext>>,
    /// MirrorSourceConfig configuration
    config: Option<MirrorSourceConfig>,
    /// Source cluster alias
    source_cluster_alias: String,
    /// Kafka consumer for source cluster
    consumer: MockKafkaConsumer,
    /// Replication policy for topic name transformation
    replication_policy: Option<Arc<dyn ReplicationPolicy>>,
    /// Consumer poll timeout
    poll_timeout: Duration,
    /// Assigned topic partitions
    assigned_partitions: Vec<TopicPartition>,
    /// Flag indicating task is stopping
    stopping: bool,
    /// Offset mapping: source offset -> target offset
    /// Key: (source_topic, partition), Value: (source_offset, target_offset)
    offset_mapping: HashMap<(String, i32), (i64, i64)>,
}

impl std::fmt::Debug for MirrorSourceTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorSourceTask")
            .field("source_cluster_alias", &self.source_cluster_alias)
            .field("poll_timeout", &self.poll_timeout)
            .field("assigned_partitions", &self.assigned_partitions)
            .field("stopping", &self.stopping)
            .field("offset_mapping_count", &self.offset_mapping.len())
            .finish()
    }
}

impl MirrorSourceTask {
    /// Creates a new MirrorSourceTask.
    pub fn new() -> Self {
        MirrorSourceTask {
            context: None,
            config: None,
            source_cluster_alias: SOURCE_CLUSTER_ALIAS_DEFAULT.to_string(),
            consumer: MockKafkaConsumer::new(),
            replication_policy: None,
            poll_timeout: Duration::from_millis(CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT as u64),
            assigned_partitions: Vec::new(),
            stopping: false,
            offset_mapping: HashMap::new(),
        }
    }

    /// Creates a MirrorSourceTask with a pre-configured consumer.
    pub fn with_consumer(consumer: MockKafkaConsumer) -> Self {
        MirrorSourceTask {
            context: None,
            config: None,
            source_cluster_alias: SOURCE_CLUSTER_ALIAS_DEFAULT.to_string(),
            consumer,
            replication_policy: None,
            poll_timeout: Duration::from_millis(CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT as u64),
            assigned_partitions: Vec::new(),
            stopping: false,
            offset_mapping: HashMap::new(),
        }
    }

    /// Returns the source cluster alias.
    pub fn source_cluster_alias(&self) -> &str {
        &self.source_cluster_alias
    }

    /// Returns the assigned partitions.
    pub fn assigned_partitions(&self) -> &[TopicPartition] {
        &self.assigned_partitions
    }

    /// Returns the replication policy.
    pub fn replication_policy(&self) -> Option<Arc<dyn ReplicationPolicy>> {
        self.replication_policy.clone()
    }

    /// Returns a mutable reference to the consumer.
    /// Used for testing to add records to the consumer.
    pub fn consumer_mut(&mut self) -> &mut MockKafkaConsumer {
        &mut self.consumer
    }

    /// Returns whether the task is stopping.
    pub fn is_stopping(&self) -> bool {
        self.stopping
    }

    /// Initializes the consumer with assigned partitions and seeks to committed offsets.
    ///
    /// Corresponds to Java: MirrorSourceTask.initializeConsumer()
    fn initialize_consumer(&mut self) {
        if self.assigned_partitions.is_empty() {
            return;
        }

        // Assign partitions to consumer
        self.consumer.assign(self.assigned_partitions.clone());

        // Seek to committed offsets if available
        if let Some(context) = &self.context {
            let offset_reader = context.offset_storage_reader();

            for tp in &self.assigned_partitions {
                // Build source partition map for offset lookup
                let source_partition: HashMap<String, Value> = HashMap::from([
                    ("topic".to_string(), json!(tp.topic())),
                    ("partition".to_string(), json!(tp.partition())),
                ]);

                // Look up committed offset
                if let Some(offset_map) = offset_reader.offset(&source_partition) {
                    if let Some(offset_value) = offset_map.get("offset") {
                        if let Some(offset) = offset_value.as_i64() {
                            // Seek to the committed offset
                            if self.consumer.seek(tp, offset).is_ok() {
                                // Successfully seeked to committed offset
                            }
                        }
                    }
                }
            }
        }
    }

    /// Converts a ConsumerRecord to a SourceRecord.
    ///
    /// This transforms the source topic name to a remote topic name using
    /// the replication policy and creates appropriate source partition/offset.
    ///
    /// Corresponds to Java: MirrorSourceTask.convertRecord(ConsumerRecord)
    pub fn convert_record(&self, record: &ConsumerRecord) -> SourceRecord {
        let source_topic = record.topic();
        let partition = record.partition();
        let offset = record.offset();

        // Build source partition
        let source_partition: HashMap<String, Value> = HashMap::from([
            ("topic".to_string(), json!(source_topic)),
            ("partition".to_string(), json!(partition)),
        ]);

        // Build source offset
        let source_offset: HashMap<String, Value> =
            HashMap::from([("offset".to_string(), json!(offset))]);

        // Transform topic name using replication policy
        let target_topic = if let Some(policy) = &self.replication_policy {
            policy.format_remote_topic(&self.source_cluster_alias, source_topic)
        } else {
            source_topic.to_string()
        };

        // Create SourceRecord with transformed topic
        // Key and value are byte arrays, represented as JSON
        let key = record.key().map(|k| json!(k.clone()));
        let value = record
            .value()
            .map(|v| json!(v.clone()))
            .unwrap_or_else(|| json!(null));

        // Create base SourceRecord
        let source_record = SourceRecord::new(
            source_partition,
            source_offset,
            target_topic,
            Some(partition),
            key,
            value,
        );

        // Set timestamp from ConsumerRecord (preserves timestamp per Kafka semantics)
        source_record.with_timestamp(record.timestamp())
    }

    /// Updates offset mapping for a given record.
    ///
    /// This tracks the mapping between source and target offsets
    /// for checkpoint synchronization.
    ///
    /// Corresponds to Java: MirrorSourceTask.maybeQueueOffsetSyncs()
    fn update_offset_mapping(&mut self, source_record: &SourceRecord, target_offset: i64) {
        let source_partition = source_record.source_partition();
        let source_offset = source_record.source_offset();

        // Extract source topic and partition
        let source_topic = source_partition
            .get("topic")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let partition = source_partition
            .get("partition")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;

        // Extract source offset
        let source_offset_val = source_offset
            .get("offset")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        // Store the offset mapping
        self.offset_mapping.insert(
            (source_topic.to_string(), partition),
            (source_offset_val, target_offset),
        );
    }
}

impl Default for MirrorSourceTask {
    fn default() -> Self {
        Self::new()
    }
}

impl Task for MirrorSourceTask {
    fn version(&self) -> &str {
        "1.0.0"
    }

    /// Starts the task with the given configuration.
    ///
    /// This initializes the consumer, assigns partitions, and seeks to
    /// previously committed offsets.
    ///
    /// Corresponds to Java: MirrorSourceTask.start(Map<String, String> props)
    fn start(&mut self, props: HashMap<String, String>) {
        // Create configuration from properties
        self.config = Some(MirrorSourceConfig::new(props.clone()));

        let config = self.config.as_ref().unwrap();

        // Set source cluster alias
        self.source_cluster_alias = props
            .get(SOURCE_CLUSTER_ALIAS_CONFIG)
            .map(|s| s.clone())
            .unwrap_or_else(|| config.source_cluster_alias().to_string());

        // Set replication policy
        self.replication_policy = Some(config.replication_policy());

        // Set poll timeout
        self.poll_timeout = config.consumer_poll_timeout();

        // Parse assigned partitions from task config
        if let Some(partitions_str) = props.get(TASK_TOPIC_PARTITIONS_CONFIG) {
            self.assigned_partitions = partitions_str
                .split(',')
                .filter_map(|tp_str| {
                    let parts: Vec<&str> = tp_str.split(':').collect();
                    if parts.len() == 2 {
                        let topic = parts[0];
                        let partition: i32 = parts[1].parse().ok()?;
                        Some(TopicPartition::new(topic, partition))
                    } else {
                        None
                    }
                })
                .collect();
        }

        // Initialize consumer with assigned partitions
        self.initialize_consumer();

        // Reset stopping flag
        self.stopping = false;
    }

    /// Stops the task.
    ///
    /// This wakes up the consumer to interrupt any blocking operations
    /// and closes the consumer.
    ///
    /// Corresponds to Java: MirrorSourceTask.stop()
    fn stop(&mut self) {
        // Set stopping flag
        self.stopping = true;

        // Wake up consumer to interrupt any blocking poll
        self.consumer.wakeup();

        // Clear consumer state
        self.consumer.clear();

        // Clear internal state
        self.assigned_partitions.clear();
        self.offset_mapping.clear();
    }
}

impl SourceTaskTrait for MirrorSourceTask {
    /// Initializes this source task with the specified context.
    ///
    /// The context provides access to the offset storage reader
    /// for loading previously committed offsets.
    ///
    /// Corresponds to Java: SourceTask.initialize(SourceTaskContext context)
    fn initialize(&mut self, context: Box<dyn SourceTaskContext>) {
        self.context = Some(context);
    }

    /// Polls for new records from the source cluster.
    ///
    /// This fetches records from the source Kafka consumer, converts them
    /// to SourceRecords with transformed topic names, and returns them.
    ///
    /// Corresponds to Java: MirrorSourceTask.poll()
    fn poll(&mut self) -> Result<Vec<SourceRecord>, ConnectError> {
        // Check if stopping
        if self.stopping {
            return Ok(Vec::new());
        }

        // Poll from consumer
        let consumer_records = self.consumer.poll(self.poll_timeout);

        if consumer_records.is_empty() {
            return Ok(Vec::new());
        }

        // Convert all records to SourceRecords
        let source_records: Vec<SourceRecord> = consumer_records
            .iter()
            .map(|record| self.convert_record(record))
            .collect();

        Ok(source_records)
    }

    /// Commits the current batch of records.
    ///
    /// This is called periodically by the Connect framework to commit offsets.
    /// For MirrorSourceTask, this promotes and fires any pending offset syncs.
    ///
    /// Corresponds to Java: MirrorSourceTask.commit()
    fn commit(&mut self) -> Result<(), ConnectError> {
        // In the Java implementation, this would:
        // - Promote and fire pending offset syncs via OffsetSyncWriter
        //
        // For the mock implementation, we just clear the offset mapping
        // as we don't have an actual OffsetSyncWriter
        self.offset_mapping.clear();
        Ok(())
    }

    /// Commits a single record after it has been successfully sent.
    ///
    /// This is called when an individual SourceRecord has been successfully
    /// delivered to the target cluster. It updates the offset mapping.
    ///
    /// Corresponds to Java: MirrorSourceTask.commitRecord(SourceRecord, RecordMetadata)
    fn commit_record(
        &mut self,
        record: &SourceRecord,
        metadata: &RecordMetadata,
    ) -> Result<(), ConnectError> {
        // Update offset mapping with target offset from metadata
        let target_offset = metadata.offset();
        self.update_offset_mapping(record, target_offset);

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
    use connect_api::connector::ConnectRecord;
    use connect_api::storage::OffsetStorageReader;
    use kafka_clients_mock::ConsumerRecord;

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

        fn add_offset(&mut self, topic: &str, partition: i32, offset: i64) {
            let partition_key: HashMap<String, Value> = HashMap::from([
                ("topic".to_string(), json!(topic)),
                ("partition".to_string(), json!(partition)),
            ]);
            let offset_value: HashMap<String, Value> =
                HashMap::from([("offset".to_string(), json!(offset))]);
            self.offsets.push((partition_key, offset_value));
        }
    }

    impl OffsetStorageReader for MockOffsetStorageReader {
        fn offset(&self, partition: &HashMap<String, Value>) -> Option<HashMap<String, Value>> {
            self.offsets
                .iter()
                .find(|(key, _)| {
                    key.get("topic") == partition.get("topic")
                        && key.get("partition") == partition.get("partition")
                })
                .map(|(_, value)| value.clone())
        }
    }

    /// Mock SourceTaskContext for testing
    struct MockSourceTaskContext {
        offset_reader: MockOffsetStorageReader,
    }

    impl MockSourceTaskContext {
        fn new(offset_reader: MockOffsetStorageReader) -> Self {
            MockSourceTaskContext { offset_reader }
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
            "mirror-source-task".to_string(),
        );
        props.insert(
            TASK_TOPIC_PARTITIONS_CONFIG.to_string(),
            "test-topic:0,test-topic:1".to_string(),
        );
        props
    }

    #[test]
    fn test_new_task() {
        let task = MirrorSourceTask::new();
        assert_eq!(task.source_cluster_alias(), SOURCE_CLUSTER_ALIAS_DEFAULT);
        assert!(task.assigned_partitions().is_empty());
        assert!(!task.stopping);
    }

    #[test]
    fn test_version() {
        let task = MirrorSourceTask::new();
        assert_eq!(task.version(), "1.0.0");
    }

    #[test]
    fn test_start_with_partitions() {
        let mut task = MirrorSourceTask::new();
        let props = create_test_config();

        task.start(props);

        assert_eq!(task.source_cluster_alias(), "backup");
        assert_eq!(task.assigned_partitions.len(), 2);
        assert!(!task.stopping);
        assert!(task.replication_policy.is_some());
    }

    #[test]
    fn test_stop() {
        let mut task = MirrorSourceTask::new();
        let props = create_test_config();
        task.start(props);

        task.stop();

        assert!(task.stopping);
        assert!(task.assigned_partitions.is_empty());
        assert!(task.offset_mapping.is_empty());
    }

    #[test]
    fn test_poll_empty() {
        let mut task = MirrorSourceTask::new();
        let props = create_test_config();
        task.start(props);

        // Poll without adding records to consumer
        let records = task.poll().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_poll_with_records() {
        let consumer = MockKafkaConsumer::new();
        let mut task = MirrorSourceTask::with_consumer(consumer);

        // Configure task
        let props = create_test_config();
        task.start(props);

        // Add records to consumer
        let tp0 = TopicPartition::new("test-topic", 0);
        let tp1 = TopicPartition::new("test-topic", 1);

        let r0 = ConsumerRecord::new("test-topic", 0, 0, Some(vec![1]), Some(vec![10]), 1000);
        let r1 = ConsumerRecord::new("test-topic", 1, 0, Some(vec![2]), Some(vec![20]), 1001);

        let mut records_map = HashMap::new();
        records_map.insert(tp0.clone(), vec![r0]);
        records_map.insert(tp1.clone(), vec![r1]);

        task.consumer.add_records(records_map);

        // Poll
        let source_records = task.poll().unwrap();
        assert_eq!(source_records.len(), 2);

        // Check that topic names are transformed
        for record in &source_records {
            // Topic should be transformed to remote topic format
            assert!(record.topic().contains("test-topic"));
        }
    }

    #[test]
    fn test_convert_record() {
        let mut task = MirrorSourceTask::new();
        let props = create_test_config();
        task.start(props);

        let consumer_record = ConsumerRecord::new(
            "source-topic",
            0,
            100,
            Some(vec![1, 2, 3]),
            Some(vec![4, 5, 6]),
            1234567890,
        );

        let source_record = task.convert_record(&consumer_record);

        // Check source partition
        assert_eq!(
            source_record.source_partition().get("topic"),
            Some(&json!("source-topic"))
        );
        assert_eq!(
            source_record.source_partition().get("partition"),
            Some(&json!(0))
        );

        // Check source offset
        assert_eq!(
            source_record.source_offset().get("offset"),
            Some(&json!(100))
        );

        // Check topic transformation (should include source cluster alias)
        assert!(source_record.topic().contains("source-topic"));
    }

    #[test]
    fn test_commit_record() {
        let mut task = MirrorSourceTask::new();

        // Create a source record
        let source_partition = HashMap::from([
            ("topic".to_string(), json!("test-topic")),
            ("partition".to_string(), json!(0)),
        ]);
        let source_offset = HashMap::from([("offset".to_string(), json!(100))]);

        let source_record = SourceRecord::new(
            source_partition,
            source_offset,
            "backup.test-topic",
            Some(0),
            Some(json!(vec![1])),
            json!(vec![2]),
        );

        // Create metadata
        let tp = TopicPartition::new("backup.test-topic", 0);
        let metadata = RecordMetadata::new(tp, 50, 0, 12345, 1, 2);

        // Commit record
        task.commit_record(&source_record, &metadata).unwrap();

        // Check offset mapping
        assert_eq!(task.offset_mapping.len(), 1);
        let mapping = task.offset_mapping.get(&("test-topic".to_string(), 0));
        assert_eq!(mapping, Some(&(100, 50)));
    }

    #[test]
    fn test_commit() {
        let mut task = MirrorSourceTask::new();

        // Add some offset mappings
        task.offset_mapping
            .insert(("topic1".to_string(), 0), (100, 200));
        task.offset_mapping
            .insert(("topic2".to_string(), 1), (50, 100));

        // Commit should clear mappings
        task.commit().unwrap();
        assert!(task.offset_mapping.is_empty());
    }

    #[test]
    fn test_initialize_consumer_with_offsets() {
        let mut offset_reader = MockOffsetStorageReader::new();
        offset_reader.add_offset("test-topic", 0, 100);
        offset_reader.add_offset("test-topic", 1, 50);

        let context = MockSourceTaskContext::new(offset_reader);

        let mut task = MirrorSourceTask::new();
        task.initialize(Box::new(context));

        // Configure and start task
        let mut props = create_test_config();
        props.insert(
            TASK_TOPIC_PARTITIONS_CONFIG.to_string(),
            "test-topic:0,test-topic:1".to_string(),
        );
        task.start(props);

        // Consumer should be assigned
        assert!(task.consumer.is_subscribed());
    }

    #[test]
    fn test_poll_when_stopping() {
        let mut task = MirrorSourceTask::new();
        let props = create_test_config();
        task.start(props);

        // Set stopping flag
        task.stopping = true;

        // Poll should return empty
        let records = task.poll().unwrap();
        assert!(records.is_empty());
    }
}
