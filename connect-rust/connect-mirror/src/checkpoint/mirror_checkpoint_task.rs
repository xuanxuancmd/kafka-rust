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

//! MirrorCheckpointTask - Source task for MirrorMaker 2 checkpoint emission.
//!
//! This module provides the MirrorCheckpointTask class that reads consumer group
//! offsets from the source cluster, translates them using OffsetSyncStore, and
//! emits Checkpoint records to the checkpoints topic.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorCheckpointTask

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common_trait::protocol::SchemaError;
use common_trait::TopicPartition;
use connect_api::connector::Task;
use connect_api::errors::ConnectError;
use connect_api::source::{SourceRecord, SourceTask as SourceTaskTrait, SourceTaskContext};
use connect_mirror_client::Checkpoint;
use connect_mirror_client::ReplicationPolicy;
use kafka_clients_mock::{MockAdmin, OffsetAndMetadata, RecordMetadata};
use serde_json::{json, Value};

use crate::config::{SOURCE_CLUSTER_ALIAS_CONFIG, SOURCE_CLUSTER_ALIAS_DEFAULT};
use crate::offset_sync::OffsetSyncStore;

use super::mirror_checkpoint_config::{
    MirrorCheckpointConfig, EMIT_CHECKPOINTS_ENABLED_CONFIG, EMIT_CHECKPOINTS_ENABLED_DEFAULT,
    EMIT_CHECKPOINTS_INTERVAL_SECONDS_CONFIG, EMIT_CHECKPOINTS_INTERVAL_SECONDS_DEFAULT,
    TASK_ASSIGNED_GROUPS_CONFIG,
};
use crate::source::topic_filter::TopicFilter;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java MirrorCheckpointTask
// ============================================================================

/// Poll timeout for reading consumer group offsets.
/// Corresponds to Java: MirrorCheckpointTask.POLL_TIMEOUT_MILLIS
pub const POLL_TIMEOUT_MILLIS_CONFIG: &str = "poll.timeout.millis";

/// Default poll timeout in milliseconds.
pub const POLL_TIMEOUT_MILLIS_DEFAULT: i64 = 1000;

/// Offset syncs topic configuration.
pub const OFFSET_SYNCS_TOPIC_CONFIG: &str = "offset.syncs.topic";

/// Default offset syncs topic name suffix.
pub const OFFSET_SYNCS_TOPIC_SUFFIX: &str = ".offset-syncs.internal";

// ============================================================================
// MirrorCheckpointTask
// ============================================================================

/// Source task for MirrorMaker 2 checkpoint emission.
///
/// The MirrorCheckpointTask is responsible for:
/// - Reading consumer group offsets from the source cluster
/// - Translating upstream offsets to downstream offsets using OffsetSyncStore
/// - Emitting Checkpoint records to the checkpoints topic
///
/// Each task is assigned a subset of consumer groups by the MirrorCheckpointConnector.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.MirrorCheckpointTask
pub struct MirrorCheckpointTask {
    /// Task context
    context: Option<Box<dyn SourceTaskContext>>,
    /// MirrorCheckpointConfig configuration
    config: Option<MirrorCheckpointConfig>,
    /// Source cluster alias
    source_cluster_alias: String,
    /// Target cluster alias
    target_cluster_alias: String,
    /// Source admin client for reading consumer group offsets
    source_admin: Arc<MockAdmin>,
    /// Target admin client (used for idle group offset sync)
    target_admin: Arc<MockAdmin>,
    /// OffsetSyncStore for translating upstream offsets to downstream offsets
    offset_sync_store: Option<OffsetSyncStore>,
    /// Replication policy for topic name transformation
    replication_policy: Option<Arc<dyn ReplicationPolicy>>,
    /// Topic filter for filtering topics
    topic_filter: Option<Arc<dyn TopicFilter>>,
    /// Assigned consumer groups
    consumer_groups: Vec<String>,
    /// Checkpoints topic name
    checkpoints_topic: String,
    /// Offset syncs topic name
    offset_syncs_topic: String,
    /// Poll timeout for reading consumer group offsets
    poll_timeout: Duration,
    /// Emit checkpoints interval
    emit_interval: Duration,
    /// Flag indicating task is stopping
    stopping: bool,
    /// Flag indicating task has been initialized
    initialized: bool,
    /// Last emit time (for interval-based emission)
    last_emit_time: Option<std::time::Instant>,
}

impl std::fmt::Debug for MirrorCheckpointTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorCheckpointTask")
            .field("source_cluster_alias", &self.source_cluster_alias)
            .field("target_cluster_alias", &self.target_cluster_alias)
            .field("consumer_groups", &self.consumer_groups)
            .field("checkpoints_topic", &self.checkpoints_topic)
            .field("offset_syncs_topic", &self.offset_syncs_topic)
            .field("stopping", &self.stopping)
            .field("initialized", &self.initialized)
            .finish()
    }
}

impl MirrorCheckpointTask {
    /// Creates a new MirrorCheckpointTask.
    pub fn new() -> Self {
        MirrorCheckpointTask {
            context: None,
            config: None,
            source_cluster_alias: SOURCE_CLUSTER_ALIAS_DEFAULT.to_string(),
            target_cluster_alias: crate::config::TARGET_CLUSTER_ALIAS_DEFAULT.to_string(),
            source_admin: Arc::new(MockAdmin::new()),
            target_admin: Arc::new(MockAdmin::new()),
            offset_sync_store: None,
            replication_policy: None,
            topic_filter: None,
            consumer_groups: Vec::new(),
            checkpoints_topic: String::new(),
            offset_syncs_topic: String::new(),
            poll_timeout: Duration::from_millis(POLL_TIMEOUT_MILLIS_DEFAULT as u64),
            emit_interval: Duration::from_secs(EMIT_CHECKPOINTS_INTERVAL_SECONDS_DEFAULT as u64),
            stopping: false,
            initialized: false,
            last_emit_time: None,
        }
    }

    /// Creates a MirrorCheckpointTask with pre-configured admin clients.
    pub fn with_admin_clients(source_admin: Arc<MockAdmin>, target_admin: Arc<MockAdmin>) -> Self {
        MirrorCheckpointTask {
            context: None,
            config: None,
            source_cluster_alias: SOURCE_CLUSTER_ALIAS_DEFAULT.to_string(),
            target_cluster_alias: crate::config::TARGET_CLUSTER_ALIAS_DEFAULT.to_string(),
            source_admin,
            target_admin,
            offset_sync_store: None,
            replication_policy: None,
            topic_filter: None,
            consumer_groups: Vec::new(),
            checkpoints_topic: String::new(),
            offset_syncs_topic: String::new(),
            poll_timeout: Duration::from_millis(POLL_TIMEOUT_MILLIS_DEFAULT as u64),
            emit_interval: Duration::from_secs(EMIT_CHECKPOINTS_INTERVAL_SECONDS_DEFAULT as u64),
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

    /// Returns the assigned consumer groups.
    pub fn consumer_groups(&self) -> &[String] {
        &self.consumer_groups
    }

    /// Returns the checkpoints topic name.
    pub fn checkpoints_topic(&self) -> &str {
        &self.checkpoints_topic
    }

    /// Returns the replication policy.
    pub fn replication_policy(&self) -> Option<Arc<dyn ReplicationPolicy>> {
        self.replication_policy.clone()
    }

    /// Returns the OffsetSyncStore.
    pub fn offset_sync_store(&self) -> Option<&OffsetSyncStore> {
        self.offset_sync_store.as_ref()
    }

    /// Returns a mutable reference to the OffsetSyncStore (for testing).
    pub fn offset_sync_store_mut(&mut self) -> Option<&mut OffsetSyncStore> {
        self.offset_sync_store.as_mut()
    }

    /// Returns whether the task has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Sets the initialized flag (for testing).
    pub fn set_initialized(&mut self, initialized: bool) {
        self.initialized = initialized;
    }

    /// Returns the offset syncs topic name.
    pub fn offset_syncs_topic(&self) -> &str {
        &self.offset_syncs_topic
    }

    /// Returns the source admin client.
    pub fn source_admin(&self) -> Arc<MockAdmin> {
        self.source_admin.clone()
    }

    /// Returns the target admin client.
    pub fn target_admin(&self) -> Arc<MockAdmin> {
        self.target_admin.clone()
    }

    /// Sets the emit interval (for testing).
    pub fn set_emit_interval(&mut self, interval: Duration) {
        self.emit_interval = interval;
    }

    /// Sets the last emit time to a past instant (for testing).
    /// This allows poll to emit immediately.
    pub fn set_last_emit_time_to_past(&mut self) {
        self.last_emit_time = Some(std::time::Instant::now() - Duration::from_secs(3600));
    }

    /// Reads consumer group offsets from the source cluster (public for testing).
    ///
    /// Uses source_admin.list_consumer_group_offsets() to get the current
    /// offsets for the specified consumer group.
    ///
    /// Corresponds to Java: MirrorCheckpointTask.listConsumerGroupOffsets()
    pub fn list_consumer_group_offsets(
        &self,
        group_id: &str,
    ) -> HashMap<TopicPartition, OffsetAndMetadata> {
        self.source_admin.list_consumer_group_offsets(group_id)
    }

    /// Creates SourceRecords for a consumer group's offsets.
    ///
    /// This method:
    /// 1. Gets consumer group offsets from source cluster
    /// 2. Filters offsets by topic filter
    /// 3. Translates upstream offsets to downstream offsets
    /// 4. Creates Checkpoint records
    ///
    /// Corresponds to Java: MirrorCheckpointTask.sourceRecordsForGroup()
    fn source_records_for_group(&self, group_id: &str) -> Result<Vec<SourceRecord>, SchemaError> {
        let mut source_records = Vec::new();

        // Get consumer group offsets from source cluster
        let offsets = self.list_consumer_group_offsets(group_id);

        // Check if offset_sync_store is initialized
        if self.offset_sync_store.is_none()
            || !self.offset_sync_store.as_ref().unwrap().is_initialized()
        {
            // OffsetSyncStore not initialized, return empty records
            return Ok(source_records);
        }

        let offset_sync_store = self.offset_sync_store.as_ref().unwrap();

        // Process each TopicPartition offset
        for (topic_partition, offset_and_metadata) in offsets.iter() {
            // Apply topic filter
            if let Some(topic_filter) = &self.topic_filter {
                if !topic_filter.should_replicate_topic(topic_partition.topic()) {
                    continue;
                }
            }

            // Get upstream offset
            let upstream_offset = offset_and_metadata.offset();

            // Translate upstream offset to downstream offset
            let downstream_offset =
                offset_sync_store.translate_offset(topic_partition, upstream_offset);

            // Only create checkpoint if downstream offset is valid (> -1)
            // downstream_offset of -1 means the upstream offset is too old
            match downstream_offset {
                Some(downstream) if downstream >= 0 => {
                    // Create Checkpoint
                    let checkpoint = Checkpoint::new(
                        group_id,
                        topic_partition.clone(),
                        upstream_offset,
                        downstream,
                        offset_and_metadata.metadata(),
                    );

                    // Convert Checkpoint to SourceRecord
                    let record = self.checkpoint_record(&checkpoint)?;
                    source_records.push(record);
                }
                Some(-1) => {
                    // Upstream offset is too old (before earliest sync)
                    // Skip this checkpoint
                }
                None => {
                    // No translation available (partition not in OffsetSyncStore)
                    // Skip this checkpoint
                }
                _ => {}
            }
        }

        Ok(source_records)
    }

    /// Creates a SourceRecord from a Checkpoint (public for testing).
    ///
    /// The SourceRecord is sent to the checkpoints topic.
    ///
    /// Corresponds to Java: MirrorCheckpointTask.checkpointRecord()
    pub fn checkpoint_record(&self, checkpoint: &Checkpoint) -> Result<SourceRecord, SchemaError> {
        // Build source partition from checkpoint's connect partition
        // Note: partition must be serialized as Number (i32), not String
        let mut source_partition: HashMap<String, Value> = HashMap::new();
        for (k, v) in checkpoint.connect_partition() {
            if k == "partition" {
                // Parse partition string to i32 for correct JSON serialization
                source_partition.insert(k, json!(v.parse::<i32>().unwrap_or(0)));
            } else {
                source_partition.insert(k, json!(v));
            }
        }

        // Build source offset - we use downstream offset as the source offset
        let source_offset: HashMap<String, Value> = HashMap::from([
            ("offset".to_string(), json!(checkpoint.downstream_offset())),
            ("metadata".to_string(), json!(checkpoint.metadata())),
        ]);

        // Serialize checkpoint key and value
        let key_bytes = checkpoint.record_key()?;
        let value_bytes = checkpoint.record_value()?;

        // Create SourceRecord
        // Key is the serialized checkpoint key (consumer group + topic + partition)
        // Value is the serialized checkpoint value (upstream + downstream offset + metadata)
        let key = Some(json!(key_bytes));
        let value = json!(value_bytes);

        Ok(SourceRecord::new(
            source_partition,
            source_offset,
            self.checkpoints_topic.clone(),
            Some(0), // Checkpoints topic has single partition
            key,
            value,
        ))
    }

    /// Starts the OffsetSyncStore (public for testing).
    ///
    /// This reads offset-syncs from the offset-syncs topic to build
    /// the internal mapping for offset translation.
    ///
    /// Returns the number of OffsetSync records read.
    pub fn start_offset_sync_store(&mut self) -> Result<usize, SchemaError> {
        if self.offset_sync_store.is_none() {
            return Ok(0);
        }

        let store = self.offset_sync_store.as_mut().unwrap();
        store.start()
    }
}

impl Default for MirrorCheckpointTask {
    fn default() -> Self {
        Self::new()
    }
}

impl Task for MirrorCheckpointTask {
    fn version(&self) -> &str {
        "1.0.0"
    }

    /// Starts the task with the given configuration.
    ///
    /// This initializes:
    /// - Source and target cluster aliases
    /// - Replication policy
    /// - Topic filter
    /// - Assigned consumer groups
    /// - OffsetSyncStore
    /// - Checkpoints topic name
    ///
    /// Corresponds to Java: MirrorCheckpointTask.start(Map<String, String> props)
    fn start(&mut self, props: HashMap<String, String>) {
        // Create configuration from properties
        self.config = Some(MirrorCheckpointConfig::new(props.clone()));

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

        // Set replication policy
        self.replication_policy = Some(config.replication_policy());

        // Set topic filter
        self.topic_filter = Some(config.topic_filter());

        // Parse assigned consumer groups from task config
        if let Some(groups_str) = props.get(TASK_ASSIGNED_GROUPS_CONFIG) {
            self.consumer_groups = groups_str
                .split(',')
                .filter_map(|g| {
                    let trimmed = g.trim();
                    if !trimmed.is_empty() {
                        Some(trimmed.to_string())
                    } else {
                        None
                    }
                })
                .collect();
        }

        // Set checkpoints topic
        self.checkpoints_topic = config.checkpoints_topic();

        // Set offset syncs topic
        // In Java, this is: replicationPolicy.offsetSyncsTopic(sourceClusterAlias)
        self.offset_syncs_topic = self
            .replication_policy
            .as_ref()
            .map(|p| p.offset_syncs_topic(&self.source_cluster_alias))
            .unwrap_or_else(|| {
                format!("{}{}", self.source_cluster_alias, OFFSET_SYNCS_TOPIC_SUFFIX)
            });

        // Set emit interval
        self.emit_interval = config.emit_checkpoints_interval();

        // Initialize OffsetSyncStore
        // In Java, this uses a KafkaBasedLog with consumer configured to read offset-syncs topic
        // For our mock, we create an OffsetSyncStore with a MockKafkaConsumer
        let consumer = kafka_clients_mock::MockKafkaConsumer::new();
        self.offset_sync_store = Some(OffsetSyncStore::new(
            consumer,
            self.offset_syncs_topic.clone(),
        ));

        // Start OffsetSyncStore - read existing offset syncs
        // Note: In Java, this is done asynchronously, but for simplicity we do it synchronously
        if let Some(store) = self.offset_sync_store.as_mut() {
            // The start() method reads from the offset-syncs topic
            // For mock implementation, we just mark as initialized
            // Tests can add syncs directly via add_sync()
            let _ = store.start();
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
    /// This closes all resources including OffsetSyncStore and admin clients.
    ///
    /// Corresponds to Java: MirrorCheckpointTask.stop()
    fn stop(&mut self) {
        // Set stopping flag
        self.stopping = true;

        // Stop OffsetSyncStore
        if let Some(store) = self.offset_sync_store.as_mut() {
            store.stop();
        }

        // Clear internal state
        self.consumer_groups.clear();
        self.initialized = false;
        self.last_emit_time = None;
    }
}

impl SourceTaskTrait for MirrorCheckpointTask {
    /// Initializes this source task with the specified context.
    ///
    /// The context provides access to the offset storage reader
    /// for loading previously committed offsets.
    ///
    /// Corresponds to Java: SourceTask.initialize(SourceTaskContext context)
    fn initialize(&mut self, context: Box<dyn SourceTaskContext>) {
        self.context = Some(context);
    }

    /// Polls for new checkpoint records to emit.
    ///
    /// This iterates through assigned consumer groups, reads their offsets
    /// from the source cluster, translates them using OffsetSyncStore, and
    /// creates Checkpoint SourceRecords.
    ///
    /// The poll() method respects the emit_checkpoints_interval configuration.
    /// If the interval has not elapsed since the last emission, returns empty.
    ///
    /// Corresponds to Java: MirrorCheckpointTask.poll()
    fn poll(&mut self) -> Result<Vec<SourceRecord>, ConnectError> {
        // Check if stopping
        if self.stopping {
            return Ok(Vec::new());
        }

        // Check if initialized
        if !self.initialized {
            return Ok(Vec::new());
        }

        // Check emit interval
        if let Some(last_emit) = self.last_emit_time {
            if std::time::Instant::now().duration_since(last_emit) < self.emit_interval {
                // Interval not elapsed, return empty
                return Ok(Vec::new());
            }
        }

        // Collect all checkpoint records from assigned consumer groups
        let mut all_records = Vec::new();

        for group_id in &self.consumer_groups {
            let records = self.source_records_for_group(group_id).map_err(|e| {
                ConnectError::data(format!("Failed to create checkpoint records: {}", e))
            })?;

            all_records.extend(records);
        }

        // Update last emit time
        self.last_emit_time = Some(std::time::Instant::now());

        Ok(all_records)
    }

    /// Commits the current batch of records.
    ///
    /// For MirrorCheckpointTask, this is a no-operation.
    /// Checkpoint records are emitted directly to the checkpoints topic.
    ///
    /// Corresponds to Java: MirrorCheckpointTask.commit()
    fn commit(&mut self) -> Result<(), ConnectError> {
        // No-op in Java implementation
        Ok(())
    }

    /// Commits a single record after it has been successfully sent.
    ///
    /// For MirrorCheckpointTask, this is a no-operation.
    ///
    /// Corresponds to Java: MirrorCheckpointTask.commitRecord(SourceRecord, RecordMetadata)
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
    use crate::source::topic_filter::DefaultTopicFilter;
    use common_trait::metrics::PluginMetrics;
    use connect_api::storage::OffsetStorageReader;
    use connect_api::ConnectRecord;
    use connect_mirror_client::OffsetSync;

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
            "mirror-checkpoint-task".to_string(),
        );
        props.insert(
            TASK_ASSIGNED_GROUPS_CONFIG.to_string(),
            "consumer-group-1,consumer-group-2".to_string(),
        );
        props
    }

    #[test]
    fn test_new_task() {
        let task = MirrorCheckpointTask::new();
        assert_eq!(task.source_cluster_alias(), SOURCE_CLUSTER_ALIAS_DEFAULT);
        assert!(task.consumer_groups().is_empty());
        assert!(!task.stopping);
        assert!(!task.initialized);
    }

    #[test]
    fn test_with_admin_clients() {
        let source_admin = Arc::new(MockAdmin::new());
        let target_admin = Arc::new(MockAdmin::new());
        let task =
            MirrorCheckpointTask::with_admin_clients(source_admin.clone(), target_admin.clone());

        assert!(Arc::ptr_eq(&task.source_admin, &source_admin));
        assert!(Arc::ptr_eq(&task.target_admin, &target_admin));
    }

    #[test]
    fn test_version() {
        let task = MirrorCheckpointTask::new();
        assert_eq!(task.version(), "1.0.0");
    }

    #[test]
    fn test_start_with_groups() {
        let mut task = MirrorCheckpointTask::new();
        let props = create_test_config();

        task.start(props);

        assert_eq!(task.source_cluster_alias(), "backup");
        assert_eq!(task.target_cluster_alias(), "primary");
        assert_eq!(task.consumer_groups.len(), 2);
        assert!(task
            .consumer_groups
            .contains(&"consumer-group-1".to_string()));
        assert!(task
            .consumer_groups
            .contains(&"consumer-group-2".to_string()));
        assert!(!task.stopping);
        assert!(task.initialized);
        assert!(task.replication_policy.is_some());
        assert!(task.topic_filter.is_some());
        assert!(task.offset_sync_store.is_some());
    }

    #[test]
    fn test_stop() {
        let mut task = MirrorCheckpointTask::new();
        let props = create_test_config();
        task.start(props);

        task.stop();

        assert!(task.stopping);
        assert!(task.consumer_groups.is_empty());
        assert!(!task.initialized);
    }

    #[test]
    fn test_poll_when_stopping() {
        let mut task = MirrorCheckpointTask::new();
        let props = create_test_config();
        task.start(props);

        task.stopping = true;

        let records = task.poll().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_poll_when_not_initialized() {
        let task = MirrorCheckpointTask::new();

        // Not initialized - poll returns empty
        // Can't call poll on non-mutable reference, so we create a mutable one
        let mut task = MirrorCheckpointTask::new();
        let records = task.poll().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_poll_with_offset_translation() {
        let source_admin = Arc::new(MockAdmin::new());

        // Add consumer group offsets to source admin
        let tp = TopicPartition::new("test-topic", 0);
        let offsets: HashMap<TopicPartition, OffsetAndMetadata> =
            HashMap::from([(tp.clone(), OffsetAndMetadata::new(100))]);
        source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

        let mut task = MirrorCheckpointTask::with_admin_clients(
            source_admin.clone(),
            Arc::new(MockAdmin::new()),
        );

        // Configure task
        let props = create_test_config();
        task.start(props);

        // Add offset sync to store
        if let Some(store) = task.offset_sync_store.as_mut() {
            store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
        }

        // Set emit interval to zero so poll will actually emit
        task.emit_interval = Duration::from_secs(0);

        // Poll should return checkpoint records
        let records = task.poll().unwrap();

        // Should have 1 checkpoint record for the consumer group
        assert_eq!(records.len(), 1);

        // Verify checkpoint record structure
        let record = &records[0];
        assert_eq!(record.topic(), task.checkpoints_topic());
    }

    #[test]
    fn test_poll_with_no_offset_sync() {
        let source_admin = Arc::new(MockAdmin::new());

        // Add consumer group offsets to source admin
        let tp = TopicPartition::new("test-topic", 0);
        let offsets: HashMap<TopicPartition, OffsetAndMetadata> =
            HashMap::from([(tp.clone(), OffsetAndMetadata::new(100))]);
        source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

        let mut task = MirrorCheckpointTask::with_admin_clients(
            source_admin.clone(),
            Arc::new(MockAdmin::new()),
        );

        // Configure task
        let props = create_test_config();
        task.start(props);

        // Don't add any offset syncs - store will have no translation

        // Set emit interval to zero so poll will actually emit
        task.emit_interval = Duration::from_secs(0);

        // Poll should return empty since no offset translation available
        let records = task.poll().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_poll_with_old_offset() {
        let source_admin = Arc::new(MockAdmin::new());

        // Add consumer group offsets to source admin - upstream offset is 50
        let tp = TopicPartition::new("test-topic", 0);
        let offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::from([
            (tp.clone(), OffsetAndMetadata::new(50)), // Old offset
        ]);
        source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

        let mut task = MirrorCheckpointTask::with_admin_clients(
            source_admin.clone(),
            Arc::new(MockAdmin::new()),
        );

        // Configure task
        let props = create_test_config();
        task.start(props);

        // Add offset sync with upstream_offset=100 (higher than consumer's 50)
        if let Some(store) = task.offset_sync_store.as_mut() {
            store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
        }

        // Set emit interval to zero so poll will actually emit
        task.emit_interval = Duration::from_secs(0);

        // Poll should return empty since consumer offset is too old
        let records = task.poll().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_list_consumer_group_offsets() {
        let source_admin = Arc::new(MockAdmin::new());

        // Add consumer group offsets
        let tp0 = TopicPartition::new("topic1", 0);
        let tp1 = TopicPartition::new("topic1", 1);
        let offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::from([
            (tp0.clone(), OffsetAndMetadata::new(100)),
            (tp1.clone(), OffsetAndMetadata::new(200)),
        ]);
        source_admin.alter_consumer_group_offsets("test-group", offsets);

        let task = MirrorCheckpointTask::with_admin_clients(
            source_admin.clone(),
            Arc::new(MockAdmin::new()),
        );

        // List offsets
        let result = task.list_consumer_group_offsets("test-group");

        assert_eq!(result.len(), 2);
        assert_eq!(result.get(&tp0).map(|o| o.offset()), Some(100));
        assert_eq!(result.get(&tp1).map(|o| o.offset()), Some(200));
    }

    #[test]
    fn test_list_consumer_group_offsets_empty() {
        let source_admin = Arc::new(MockAdmin::new());
        let task = MirrorCheckpointTask::with_admin_clients(
            source_admin.clone(),
            Arc::new(MockAdmin::new()),
        );

        // List offsets for non-existent group
        let result = task.list_consumer_group_offsets("non-existent-group");

        assert!(result.is_empty());
    }

    #[test]
    fn test_checkpoint_record() {
        let mut task = MirrorCheckpointTask::new();
        let props = create_test_config();
        task.start(props);

        let tp = TopicPartition::new("test-topic", 0);
        let checkpoint = Checkpoint::new("test-group", tp, 100, 200, "test-metadata");

        let record = task.checkpoint_record(&checkpoint).unwrap();

        // Verify record structure
        assert_eq!(record.topic(), task.checkpoints_topic());
        assert_eq!(record.kafka_partition(), Some(0));

        // Source partition should contain group, topic, partition
        let source_partition = record.source_partition();
        assert_eq!(source_partition.get("group"), Some(&json!("test-group")));
        assert_eq!(source_partition.get("topic"), Some(&json!("test-topic")));
        assert_eq!(source_partition.get("partition"), Some(&json!(0)));
    }

    #[test]
    fn test_commit() {
        let mut task = MirrorCheckpointTask::new();
        let result = task.commit();
        assert!(result.is_ok());
    }

    #[test]
    fn test_commit_record() {
        let mut task = MirrorCheckpointTask::new();

        let source_partition = HashMap::from([
            ("group".to_string(), json!("test-group")),
            ("topic".to_string(), json!("test-topic")),
            ("partition".to_string(), json!(0)),
        ]);
        let source_offset = HashMap::from([("offset".to_string(), json!(200))]);

        let record = SourceRecord::new(
            source_partition,
            source_offset,
            "checkpoints-topic",
            Some(0),
            Some(json!(vec![1, 2, 3])),
            json!(vec![4, 5, 6]),
        );

        let tp = TopicPartition::new("checkpoints-topic", 0);
        let metadata = RecordMetadata::new(tp, 0, 0, 12345, 1, 2);

        let result = task.commit_record(&record, &metadata);
        assert!(result.is_ok());
    }

    #[test]
    fn test_initialize() {
        let mut task = MirrorCheckpointTask::new();
        let context = Box::new(MockSourceTaskContext::new());

        task.initialize(context);

        assert!(task.context.is_some());
    }

    #[test]
    fn test_emit_interval_respected() {
        let mut task = MirrorCheckpointTask::new();
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
    fn test_multiple_consumer_groups() {
        let source_admin = Arc::new(MockAdmin::new());

        // Add offsets for multiple consumer groups
        let tp = TopicPartition::new("test-topic", 0);

        let offsets1: HashMap<TopicPartition, OffsetAndMetadata> =
            HashMap::from([(tp.clone(), OffsetAndMetadata::new(100))]);
        source_admin.alter_consumer_group_offsets("consumer-group-1", offsets1);

        let offsets2: HashMap<TopicPartition, OffsetAndMetadata> =
            HashMap::from([(tp.clone(), OffsetAndMetadata::new(150))]);
        source_admin.alter_consumer_group_offsets("consumer-group-2", offsets2);

        let mut task = MirrorCheckpointTask::with_admin_clients(
            source_admin.clone(),
            Arc::new(MockAdmin::new()),
        );

        // Configure task with both groups
        let props = create_test_config();
        task.start(props);

        // Add offset sync
        if let Some(store) = task.offset_sync_store.as_mut() {
            store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
        }

        // Set emit interval to zero
        task.emit_interval = Duration::from_secs(0);

        // Poll should return 2 checkpoint records (one for each group)
        let records = task.poll().unwrap();

        // Note: consumer-group-2 has offset 150 which is after sync at 100,
        // so it will get translated to 201 (downstream + 1)
        assert!(!records.is_empty());
    }

    #[test]
    fn test_topic_filter_excluded() {
        let source_admin = Arc::new(MockAdmin::new());

        // Add offsets for a topic that should be filtered out
        let tp_internal = TopicPartition::new("mm2-offset-syncs.backup.internal", 0);
        let tp_normal = TopicPartition::new("test-topic", 0);

        let offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::from([
            (tp_internal.clone(), OffsetAndMetadata::new(100)),
            (tp_normal.clone(), OffsetAndMetadata::new(200)),
        ]);
        source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

        let mut task = MirrorCheckpointTask::with_admin_clients(
            source_admin.clone(),
            Arc::new(MockAdmin::new()),
        );

        // Configure task
        let mut props = create_test_config();
        props.insert(
            crate::source::topic_filter::TOPICS_INCLUDE_CONFIG.to_string(),
            "test-topic".to_string(),
        );
        task.start(props);

        // Add offset syncs for both partitions
        if let Some(store) = task.offset_sync_store.as_mut() {
            store.add_sync(OffsetSync::new(tp_internal.clone(), 100, 200));
            store.add_sync(OffsetSync::new(tp_normal.clone(), 200, 400));
        }

        // Set emit interval to zero
        task.emit_interval = Duration::from_secs(0);

        // Poll should only return checkpoint for test-topic (internal topic filtered)
        let records = task.poll().unwrap();

        // Should have 1 record (test-topic only, internal topic excluded by filter)
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn test_checkpoints_topic_name() {
        let mut task = MirrorCheckpointTask::new();
        let props = create_test_config();
        task.start(props);

        // Checkpoints topic should be formatted with source cluster alias
        // DefaultReplicationPolicy: sourceAlias.checkpoints.internal
        assert_eq!(task.checkpoints_topic(), "backup.checkpoints.internal");
    }

    #[test]
    fn test_offset_syncs_topic_name() {
        let mut task = MirrorCheckpointTask::new();
        let props = create_test_config();
        task.start(props);

        // Offset syncs topic should be formatted with source cluster alias
        // DefaultReplicationPolicy: sourceAlias.offset-syncs.internal
        assert!(task.offset_syncs_topic.contains("backup"));
        assert!(task.offset_syncs_topic.contains("offset-syncs"));
    }

    #[test]
    fn test_start_offset_sync_store() {
        let mut task = MirrorCheckpointTask::new();
        let props = create_test_config();
        task.start(props);

        // Start OffsetSyncStore (reads from topic)
        let result = task.start_offset_sync_store();
        assert!(result.is_ok());
    }
}
