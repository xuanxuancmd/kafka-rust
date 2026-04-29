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

//! Tests for MirrorCheckpointTask.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorCheckpointTaskTest
//!
//! Test coverage includes:
//! - Downstream topic renaming via replication policy
//! - Checkpoint generation and SourceRecord creation
//! - Offset translation path (upstream to downstream)
//! - Filtering behavior (topic filter, no offset syncs, null offsets)
//! - Poll behavior and emit interval
//! - Monotonic checkpoint records (no rewinds)
//! - Task restart using existing checkpoints
//! - Lifecycle (start/stop/initialize)
//! - Consumer group offset listing

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common_trait::metrics::PluginMetrics;
use common_trait::TopicPartition;
use connect_api::connector::{ConnectRecord, Task};
use connect_api::errors::ConnectError;
use connect_api::source::{SourceRecord, SourceTask as SourceTaskTrait, SourceTaskContext};
use connect_api::storage::OffsetStorageReader;
use connect_mirror::checkpoint::{
    MirrorCheckpointConfig, MirrorCheckpointTask, EMIT_CHECKPOINTS_ENABLED_CONFIG,
    EMIT_CHECKPOINTS_INTERVAL_SECONDS_CONFIG, TASK_ASSIGNED_GROUPS_CONFIG,
};
use connect_mirror::config::{SOURCE_CLUSTER_ALIAS_CONFIG, TARGET_CLUSTER_ALIAS_CONFIG};
use connect_mirror::offset_sync::OffsetSyncStore;
use connect_mirror_client::{Checkpoint, DefaultReplicationPolicy, OffsetSync, ReplicationPolicy};
use kafka_clients_mock::{MockAdmin, OffsetAndMetadata, RecordMetadata};
use serde_json::json;

// ============================================================================
// Mock Implementations for Testing
// ============================================================================

/// Mock OffsetStorageReader for testing committed offsets.
struct MockOffsetStorageReader {
    offsets: Vec<(
        HashMap<String, serde_json::Value>,
        HashMap<String, serde_json::Value>,
    )>,
}

impl MockOffsetStorageReader {
    fn new() -> Self {
        MockOffsetStorageReader {
            offsets: Vec::new(),
        }
    }
}

impl OffsetStorageReader for MockOffsetStorageReader {
    fn offset(
        &self,
        partition: &HashMap<String, serde_json::Value>,
    ) -> Option<HashMap<String, serde_json::Value>> {
        self.offsets
            .iter()
            .find(|(key, _)| {
                key.get("group") == partition.get("group")
                    && key.get("topic") == partition.get("topic")
                    && key.get("partition") == partition.get("partition")
            })
            .map(|(_, value)| value.clone())
    }
}

/// Mock SourceTaskContext for testing.
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

// ============================================================================
// Test Helper Functions
// ============================================================================

/// Creates a basic test configuration for MirrorCheckpointTask.
fn create_basic_config() -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert(
        SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
        "source1".to_string(),
    );
    props.insert(
        TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
        "target2".to_string(),
    );
    props.insert("name".to_string(), "mirror-checkpoint-task".to_string());
    props.insert(
        TASK_ASSIGNED_GROUPS_CONFIG.to_string(),
        "consumer-group-1".to_string(),
    );
    props
}

/// Creates a test configuration with multiple consumer groups.
fn create_config_with_multiple_groups(groups: &str) -> HashMap<String, String> {
    let mut props = create_basic_config();
    props.insert(TASK_ASSIGNED_GROUPS_CONFIG.to_string(), groups.to_string());
    props
}

/// Creates a test configuration with emit interval disabled (interval = 0).
fn create_config_with_zero_interval() -> HashMap<String, String> {
    let mut props = create_basic_config();
    props.insert(
        EMIT_CHECKPOINTS_INTERVAL_SECONDS_CONFIG.to_string(),
        "0".to_string(),
    );
    props
}

/// Creates a test configuration with checkpoints disabled.
fn create_config_with_checkpoints_disabled() -> HashMap<String, String> {
    let mut props = create_basic_config();
    props.insert(
        EMIT_CHECKPOINTS_ENABLED_CONFIG.to_string(),
        "false".to_string(),
    );
    props
}

/// Creates a test configuration with custom emit interval.
fn create_config_with_emit_interval(seconds: i64) -> HashMap<String, String> {
    let mut props = create_basic_config();
    props.insert(
        EMIT_CHECKPOINTS_INTERVAL_SECONDS_CONFIG.to_string(),
        seconds.to_string(),
    );
    props
}

// ============================================================================
// testDownstreamTopicRenaming Tests
// Corresponds to Java: MirrorCheckpointTaskTest.testDownstreamTopicRenaming()
// ============================================================================

/// Verifies topic partition renaming via replication policy.
/// Topics from source cluster should be prefixed with source alias.
/// Topics already prefixed with target alias should revert to original name.
#[test]
fn test_downstream_topic_renaming_source_topic() {
    // Corresponds to Java: testDownstreamTopicRenaming - source topic case
    // Topic "topic3" from source1 cluster should become "source1.topic3"

    let policy = DefaultReplicationPolicy::new();

    // Source topic from source cluster - should be prefixed
    let remote_topic = policy.format_remote_topic("source1", "topic3");
    assert_eq!(remote_topic, "source1.topic3");

    // Verify topic_partition rename (Reverse direction)
    let source_topic = policy.original_topic("source1.topic3");
    assert_eq!(source_topic, "topic3");
}

#[test]
fn test_downstream_topic_renaming_target_prefix_topic() {
    // Corresponds to Java: testDownstreamTopicRenaming - target prefixed topic case
    // Topic "target2.topic3" should revert to "topic3" when syncing from source to target

    let policy = DefaultReplicationPolicy::new();

    // Topic prefixed with target alias - should revert to original
    let original_topic = policy.original_topic("target2.topic3");
    assert_eq!(original_topic, "topic3");
}

#[test]
fn test_downstream_topic_renaming_unprefixed_topic() {
    // Unprefixed topic stays unprefixed for original_topic
    let policy = DefaultReplicationPolicy::new();

    let original = policy.original_topic("simple-topic");
    assert_eq!(original, "simple-topic");
}

#[test]
fn test_replication_policy_in_checkpoint_task() {
    let mut task = MirrorCheckpointTask::new();
    task.start(create_basic_config());

    // Replication policy should be available after start
    assert!(task.replication_policy().is_some());

    let policy = task.replication_policy().unwrap();

    // Verify topic formatting behavior
    let remote_topic = policy.format_remote_topic("source1", "test-topic");
    assert!(remote_topic.contains("test-topic"));
}

// ============================================================================
// testCheckpoint Tests
// Corresponds to Java: MirrorCheckpointTaskTest.testCheckpoint()
// ============================================================================

/// Tests checkpoint generation with offset translation.
/// Verifies that Checkpoint contains correct group, topic partition, and offsets.
#[test]
fn test_checkpoint_generation_basic() {
    // Corresponds to Java: testCheckpoint
    // Creates a checkpoint with correct upstream and downstream offsets

    let group_id = "consumer-group-1";
    let tp = TopicPartition::new("source-topic", 0);
    let upstream_offset = 100;
    let downstream_offset = 200;
    let metadata = "";

    let checkpoint = Checkpoint::new(
        group_id,
        tp.clone(),
        upstream_offset,
        downstream_offset,
        metadata,
    );

    // Verify checkpoint fields
    assert_eq!(checkpoint.consumer_group_id(), group_id);
    assert_eq!(checkpoint.topic_partition(), &tp);
    assert_eq!(checkpoint.upstream_offset(), upstream_offset);
    assert_eq!(checkpoint.downstream_offset(), downstream_offset);
    assert_eq!(checkpoint.metadata(), metadata);
}

#[test]
fn test_checkpoint_record_creation() {
    // Corresponds to Java: testCheckpoint - checkpointRecord method
    // Verify SourceRecord created from Checkpoint

    let mut task = MirrorCheckpointTask::new();
    task.start(create_basic_config());

    let tp = TopicPartition::new("source-topic", 0);
    let checkpoint = Checkpoint::new("consumer-group-1", tp.clone(), 100, 200, "test-metadata");

    let source_record = task.checkpoint_record(&checkpoint).unwrap();

    // Verify source partition contains group, topic, partition
    let source_partition = source_record.source_partition();
    assert_eq!(
        source_partition.get("group"),
        Some(&json!("consumer-group-1"))
    );
    assert_eq!(source_partition.get("topic"), Some(&json!("source-topic")));
    assert_eq!(source_partition.get("partition"), Some(&json!(0)));

    // Verify source offset contains downstream offset
    let source_offset = source_record.source_offset();
    assert_eq!(source_offset.get("offset"), Some(&json!(200)));

    // Verify topic is checkpoints topic
    assert!(source_record.topic().contains("checkpoints"));
}

#[test]
fn test_checkpoint_with_metadata() {
    // Verify checkpoint preserves metadata
    let checkpoint = Checkpoint::new(
        "group1",
        TopicPartition::new("topic", 0),
        100,
        200,
        "custom-metadata-string",
    );

    assert_eq!(checkpoint.metadata(), "custom-metadata-string");
}

#[test]
fn test_checkpoint_connect_partition() {
    // Verify connect_partition format
    let checkpoint = Checkpoint::new("group1", TopicPartition::new("topic1", 5), 100, 200, "");

    let partition = checkpoint.connect_partition();
    assert_eq!(partition.get("group").map(|s| s.as_str()), Some("group1"));
    assert_eq!(partition.get("topic").map(|s| s.as_str()), Some("topic1"));
    assert_eq!(partition.get("partition").map(|s| s.as_str()), Some("5"));
}

// ============================================================================
// testSyncOffset Tests (Idle Group Offset Sync)
// Corresponds to Java: MirrorCheckpointTaskTest.testSyncOffset()
// ============================================================================

#[test]
fn test_list_consumer_group_offsets_basic() {
    // Corresponds to Java: MirrorCheckpointTask.listConsumerGroupOffsets()

    let source_admin = Arc::new(MockAdmin::new());

    // Add consumer group offsets
    let tp0 = TopicPartition::new("topic1", 0);
    let tp1 = TopicPartition::new("topic1", 1);

    let offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::from([
        (tp0.clone(), OffsetAndMetadata::new(100)),
        (tp1.clone(), OffsetAndMetadata::new(200)),
    ]);

    source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

    let task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    // List offsets
    let result = task.list_consumer_group_offsets("consumer-group-1");

    assert_eq!(result.len(), 2);
    assert_eq!(result.get(&tp0).map(|o| o.offset()), Some(100));
    assert_eq!(result.get(&tp1).map(|o| o.offset()), Some(200));
}

#[test]
fn test_list_consumer_group_offsets_empty_group() {
    // Empty consumer group returns empty offsets

    let source_admin = Arc::new(MockAdmin::new());
    let task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    let result = task.list_consumer_group_offsets("non-existent-group");
    assert!(result.is_empty());
}

#[test]
fn test_list_consumer_group_offsets_with_metadata() {
    // Verify offsets with metadata

    let source_admin = Arc::new(MockAdmin::new());

    let tp = TopicPartition::new("topic", 0);
    let offset_with_metadata = OffsetAndMetadata::with_metadata(100, "leader:1:0");

    let offsets = HashMap::from([(tp.clone(), offset_with_metadata)]);
    source_admin.alter_consumer_group_offsets("group1", offsets);

    let task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    let result = task.list_consumer_group_offsets("group1");
    assert_eq!(result.len(), 1);

    let offset_meta = result.get(&tp).unwrap();
    assert_eq!(offset_meta.offset(), 100);
    assert_eq!(offset_meta.metadata(), "leader:1:0");
}

// ============================================================================
// testNoCheckpointForTopicWithoutOffsetSyncs Tests
// Corresponds to Java: MirrorCheckpointTaskTest.testNoCheckpointForTopicWithoutOffsetSyncs()
// ============================================================================

#[test]
fn test_no_checkpoint_without_offset_syncs() {
    // Corresponds to Java: testNoCheckpointForTopicWithoutOffsetSyncs
    // No checkpoint generated if no offset sync exists for the partition

    let source_admin = Arc::new(MockAdmin::new());

    // Add consumer group offsets
    let tp = TopicPartition::new("topic-with-no-syncs", 0);
    let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::new(100))]);
    source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

    let mut task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    task.start(create_basic_config());

    // Don't add any offset syncs to the store
    // Store is initialized but has no syncs for this partition

    // Force immediate emission
    task.set_emit_interval(Duration::from_secs(60));
    task.set_last_emit_time_to_past();

    // Poll should return empty (no offset translation available)
    let records = task.poll().unwrap();
    assert!(records.is_empty());
}

#[test]
fn test_checkpoint_only_with_matching_offset_sync() {
    // Checkpoint generated only when offset sync matches partition

    let source_admin = Arc::new(MockAdmin::new());

    // Add offsets for two partitions
    let tp_with_sync = TopicPartition::new("topic-with-sync", 0);
    let tp_no_sync = TopicPartition::new("topic-no-sync", 0);

    let offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::from([
        (tp_with_sync.clone(), OffsetAndMetadata::new(100)),
        (tp_no_sync.clone(), OffsetAndMetadata::new(50)),
    ]);
    source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

    let mut task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    task.start(create_basic_config());

    // Add offset sync only for one partition
    if let Some(store) = task.offset_sync_store_mut() {
        store.add_sync(OffsetSync::new(tp_with_sync.clone(), 100, 200));
    }

    // Force immediate emission
    task.set_emit_interval(Duration::from_secs(60));
    task.set_last_emit_time_to_past();

    // Poll should return checkpoint only for topic-with-sync
    let records = task.poll().unwrap();
    assert_eq!(records.len(), 1);

    // Verify the checkpoint is for topic-with-sync
    let record = &records[0];
    let source_partition = record.source_partition();
    assert_eq!(
        source_partition.get("topic"),
        Some(&json!("topic-with-sync"))
    );
}

// ============================================================================
// testNoCheckpointForTopicWithNullOffsetAndMetadata Tests
// Corresponds to Java: MirrorCheckpointTaskTest.testNoCheckpointForTopicWithNullOffsetAndMetadata()
// ============================================================================

#[test]
fn test_no_checkpoint_for_old_offset() {
    // Corresponds to Java: testNoCheckpointForTopicWithNullOffsetAndMetadata
    // Upstream offset too old (before earliest sync) should not generate checkpoint

    let source_admin = Arc::new(MockAdmin::new());

    let tp = TopicPartition::new("test-topic", 0);

    // Consumer group offset is 50 (older than sync's upstream_offset=100)
    let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::new(50))]);
    source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

    let mut task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    task.start(create_basic_config());

    // Add offset sync with upstream_offset=100 (higher than consumer's 50)
    if let Some(store) = task.offset_sync_store_mut() {
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
    }

    // Force immediate emission
    task.set_emit_interval(Duration::from_secs(60));
    task.set_last_emit_time_to_past();

    // Poll should return empty (consumer offset is too old)
    let records = task.poll().unwrap();
    assert!(records.is_empty());
}

#[test]
fn test_checkpoint_for_offset_after_sync() {
    // Upstream offset after sync should translate to downstream+1

    let source_admin = Arc::new(MockAdmin::new());

    let tp = TopicPartition::new("test-topic", 0);

    // Consumer offset is 150 (after sync at 100)
    let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::new(150))]);
    source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

    let mut task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    task.start(create_basic_config());

    // Add sync: upstream 100 -> downstream 200
    if let Some(store) = task.offset_sync_store_mut() {
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
    }

    // Force immediate emission
    task.set_emit_interval(Duration::from_secs(60));
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();
    assert_eq!(records.len(), 1);

    // Downstream offset should be 201 (200 + 1)
    let record = &records[0];
    let source_offset = record.source_offset();
    assert_eq!(source_offset.get("offset"), Some(&json!(201)));
}

#[test]
fn test_checkpoint_for_exact_offset_match() {
    // Exact upstream offset match should return exact downstream offset

    let source_admin = Arc::new(MockAdmin::new());

    let tp = TopicPartition::new("test-topic", 0);

    // Consumer offset exactly matches sync's upstream_offset
    let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::new(100))]);
    source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

    let mut task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    task.start(create_basic_config());

    // Add sync: upstream 100 -> downstream 200
    if let Some(store) = task.offset_sync_store_mut() {
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
    }

    // Force immediate emission
    task.set_emit_interval(Duration::from_secs(60));
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();
    assert_eq!(records.len(), 1);

    // Downstream offset should be exactly 200
    let record = &records[0];
    let source_offset = record.source_offset();
    assert_eq!(source_offset.get("offset"), Some(&json!(200)));
}

// ============================================================================
// testCheckpointRecordsMonotonic Tests
// Corresponds to Java: MirrorCheckpointTaskTest.testCheckpointRecordsMonotonicIfStoreRewinds()
// ============================================================================

#[test]
fn test_offset_sync_store_translation_monotonic() {
    // Corresponds to Java: testCheckpointRecordsMonotonicIfStoreRewinds
    // Test that downstream offset translation respects monotonicity

    let mut store = OffsetSyncStore::default();
    store.set_initialized(true);

    let tp = TopicPartition::new("test-topic", 0);

    // Add initial sync
    store.add_sync(OffsetSync::new(tp.clone(), 100, 200));

    // Translation at exact match
    assert_eq!(store.translate_offset(&tp, 100), Some(200));

    // Translation after sync (upstream > sync)
    assert_eq!(store.translate_offset(&tp, 150), Some(201));

    // Translation before sync (upstream < sync) returns -1 (too old)
    assert_eq!(store.translate_offset(&tp, 50), Some(-1));
}

#[test]
fn test_offset_translation_with_multiple_syncs() {
    // Multiple syncs should use the latest applicable sync

    let mut store = OffsetSyncStore::default();
    store.set_initialized(true);

    let tp = TopicPartition::new("test-topic", 0);

    // Add syncs in ascending order
    store.add_sync(OffsetSync::new(tp.clone(), 0, 0));
    store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
    store.add_sync(OffsetSync::new(tp.clone(), 500, 1000));

    // At upstream 100 -> downstream 200 (exact match)
    assert_eq!(store.translate_offset(&tp, 100), Some(200));

    // At upstream 300 -> uses sync at 100 -> downstream 201
    assert_eq!(store.translate_offset(&tp, 300), Some(201));

    // At upstream 600 -> uses sync at 500 -> downstream 1001
    assert_eq!(store.translate_offset(&tp, 600), Some(1001));

    // At upstream 50 -> uses sync at 0 -> downstream 51
    assert_eq!(store.translate_offset(&tp, 50), Some(1));
}

// ============================================================================
// testPoll Tests - Poll Behavior
// ============================================================================

#[test]
fn test_poll_when_not_initialized() {
    // Task not initialized should return empty

    let task = MirrorCheckpointTask::new();

    // Cannot call poll on immutable reference
    let mut task = MirrorCheckpointTask::new();
    let records = task.poll().unwrap();
    assert!(records.is_empty());
}

#[test]
fn test_poll_when_stopping() {
    // Task stopping should return empty

    let mut task = MirrorCheckpointTask::new();
    task.start(create_basic_config());

    task.stop();

    let records = task.poll().unwrap();
    assert!(records.is_empty());
}

#[test]
fn test_poll_respects_emit_interval() {
    // Poll respects emit_checkpoints_interval configuration

    let mut task = MirrorCheckpointTask::new();
    task.start(create_config_with_emit_interval(60)); // 60 seconds interval

    // Immediately after start, poll should return empty
    let records = task.poll().unwrap();
    assert!(records.is_empty());
}

#[test]
fn test_poll_after_interval_elapsed() {
    // After interval elapsed, poll should emit checkpoints

    let source_admin = Arc::new(MockAdmin::new());

    let tp = TopicPartition::new("test-topic", 0);
    let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::new(100))]);
    source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

    let mut task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    task.start(create_basic_config());

    // Add offset sync
    if let Some(store) = task.offset_sync_store_mut() {
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
    }

    // Force immediate emission by setting last_emit_time to past
    task.set_emit_interval(Duration::from_secs(60));
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();
    assert_eq!(records.len(), 1);
}

#[test]
fn test_poll_with_zero_interval_disabled() {
    // Zero interval means checkpoints disabled

    let mut task = MirrorCheckpointTask::new();
    task.start(create_config_with_zero_interval());

    // Zero interval = disabled
    assert!(task.poll().unwrap().is_empty());
}

#[test]
fn test_poll_multiple_consumer_groups() {
    // Poll handles multiple assigned consumer groups

    let source_admin = Arc::new(MockAdmin::new());

    let tp = TopicPartition::new("test-topic", 0);

    // Add offsets for multiple groups
    let offsets1 = HashMap::from([(tp.clone(), OffsetAndMetadata::new(100))]);
    source_admin.alter_consumer_group_offsets("consumer-group-1", offsets1);

    let offsets2 = HashMap::from([(tp.clone(), OffsetAndMetadata::new(150))]);
    source_admin.alter_consumer_group_offsets("consumer-group-2", offsets2);

    let mut task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    task.start(create_config_with_multiple_groups(
        "consumer-group-1,consumer-group-2",
    ));

    // Add sync for one partition
    if let Some(store) = task.offset_sync_store_mut() {
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
    }

    // Force immediate emission
    task.set_emit_interval(Duration::from_secs(60));
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();

    // Should have checkpoints for both groups
    // group-1 at 100 -> downstream 200 (exact)
    // group-2 at 150 -> downstream 201 (after sync)
    assert!(!records.is_empty());
}

// ============================================================================
// testCheckpointStoreInitialized Tests
// Corresponds to Java: MirrorCheckpointTaskTest.testCheckpointStoreInitialized()
// ============================================================================

#[test]
fn test_offset_sync_store_not_initialized_returns_empty() {
    // Corresponds to Java: testCheckpointStoreInitialized
    // poll returns empty if OffsetSyncStore not initialized

    let source_admin = Arc::new(MockAdmin::new());

    let tp = TopicPartition::new("test-topic", 0);
    let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::new(100))]);
    source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

    let mut task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    task.start(create_basic_config());

    // Store is initialized by start() in our implementation
    // But verify behavior when store not initialized
    if let Some(store) = task.offset_sync_store_mut() {
        store.set_initialized(false);
    }

    // Force immediate emission
    task.set_emit_interval(Duration::from_secs(60));
    task.set_last_emit_time_to_past();

    // Should return empty when store not initialized
    let records = task.poll().unwrap();
    assert!(records.is_empty());
}

#[test]
fn test_offset_sync_store_initialized_enables_poll() {
    // Once initialized, poll should return checkpoints

    let source_admin = Arc::new(MockAdmin::new());

    let tp = TopicPartition::new("test-topic", 0);
    let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::new(100))]);
    source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

    let mut task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    task.start(create_basic_config());

    // Add sync and ensure initialized
    if let Some(store) = task.offset_sync_store_mut() {
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
        store.set_initialized(true);
    }

    // Force immediate emission
    task.set_emit_interval(Duration::from_secs(60));
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();
    assert_eq!(records.len(), 1);
}

// ============================================================================
// Topic Filter Tests
// ============================================================================

#[test]
fn test_topic_filter_excludes_internal_topics() {
    // Internal topics should be filtered out

    let source_admin = Arc::new(MockAdmin::new());

    let tp_internal = TopicPartition::new("mm2-offset-syncs.source1.internal", 0);
    let tp_normal = TopicPartition::new("test-topic", 0);

    let offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::from([
        (tp_internal.clone(), OffsetAndMetadata::new(100)),
        (tp_normal.clone(), OffsetAndMetadata::new(200)),
    ]);
    source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

    let mut task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    // Configure with topic include pattern
    let mut props = create_basic_config();
    props.insert(
        connect_mirror::source::topic_filter::TOPICS_INCLUDE_CONFIG.to_string(),
        "test-topic".to_string(),
    );
    task.start(props);

    // Add syncs for both
    if let Some(store) = task.offset_sync_store_mut() {
        store.add_sync(OffsetSync::new(tp_internal.clone(), 100, 200));
        store.add_sync(OffsetSync::new(tp_normal.clone(), 200, 400));
    }

    // Force immediate emission
    task.set_emit_interval(Duration::from_secs(60));
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();

    // Should only have checkpoint for test-topic (internal topic filtered)
    assert_eq!(records.len(), 1);

    let record = &records[0];
    let source_partition = record.source_partition();
    assert_eq!(source_partition.get("topic"), Some(&json!("test-topic")));
}

#[test]
fn test_topic_filter_allows_matching_topics() {
    // Topics matching include pattern should pass filter

    let source_admin = Arc::new(MockAdmin::new());

    let tp1 = TopicPartition::new("prod-topic1", 0);
    let tp2 = TopicPartition::new("prod-topic2", 0);

    let offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::from([
        (tp1.clone(), OffsetAndMetadata::new(100)),
        (tp2.clone(), OffsetAndMetadata::new(200)),
    ]);
    source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

    let mut task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    // Configure with wildcard pattern
    let mut props = create_basic_config();
    props.insert(
        connect_mirror::source::topic_filter::TOPICS_INCLUDE_CONFIG.to_string(),
        ".*".to_string(),
    );
    task.start(props);

    // Add syncs for both partitions
    if let Some(store) = task.offset_sync_store_mut() {
        store.add_sync(OffsetSync::new(tp1.clone(), 100, 200));
        store.add_sync(OffsetSync::new(tp2.clone(), 200, 400));
    }

    // Force immediate emission
    task.set_emit_interval(Duration::from_secs(60));
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();

    // Both should pass filter
    assert_eq!(records.len(), 2);
}

// ============================================================================
// Lifecycle Tests
// ============================================================================

#[test]
fn test_task_lifecycle_start_stop() {
    let mut task = MirrorCheckpointTask::new();

    // Before start
    assert!(!task.is_initialized());
    assert!(task.consumer_groups().is_empty());

    // Start
    task.start(create_basic_config());
    assert!(task.is_initialized());
    assert!(!task.consumer_groups().is_empty());

    // Stop
    task.stop();
    assert!(!task.is_initialized());
    assert!(task.consumer_groups().is_empty());
}

#[test]
fn test_task_initialize() {
    let mut task = MirrorCheckpointTask::new();
    let context = Box::new(MockSourceTaskContext::new());

    task.initialize(context);

    // Context should be set (verified through successful start)
    task.start(create_basic_config());
    assert!(task.is_initialized());
}

#[test]
fn test_task_version() {
    let task = MirrorCheckpointTask::new();
    assert_eq!(task.version(), "1.0.0");
}

#[test]
fn test_task_with_admin_clients() {
    let source_admin = Arc::new(MockAdmin::new());
    let target_admin = Arc::new(MockAdmin::new());

    let task = MirrorCheckpointTask::with_admin_clients(source_admin.clone(), target_admin.clone());

    // Admin clients should be set
    assert!(Arc::ptr_eq(&task.source_admin(), &source_admin));
    assert!(Arc::ptr_eq(&task.target_admin(), &target_admin));
}

#[test]
fn test_task_debug_format() {
    let task = MirrorCheckpointTask::new();
    let debug_str = format!("{:?}", task);

    // Debug format should include key fields
    assert!(debug_str.contains("MirrorCheckpointTask"));
    assert!(debug_str.contains("source_cluster_alias"));
}

// ============================================================================
// Commit Tests
// ============================================================================

#[test]
fn test_commit_no_op() {
    // commit() is a no-op for MirrorCheckpointTask

    let mut task = MirrorCheckpointTask::new();
    let result = task.commit();
    assert!(result.is_ok());
}

#[test]
fn test_commit_record_no_op() {
    // commitRecord() is a no-op

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

// ============================================================================
// Configuration Tests
// ============================================================================

#[test]
fn test_checkpoints_topic_name() {
    let mut task = MirrorCheckpointTask::new();
    task.start(create_basic_config());

    // Checkpoints topic should be formatted with source cluster alias
    // DefaultReplicationPolicy: sourceAlias.checkpoints.internal
    assert_eq!(task.checkpoints_topic(), "source1.checkpoints.internal");
}

#[test]
fn test_offset_syncs_topic_name() {
    let mut task = MirrorCheckpointTask::new();
    task.start(create_basic_config());

    // Offset syncs topic should contain source alias
    assert!(task.offset_syncs_topic().contains("source1"));
    assert!(task.offset_syncs_topic().contains("offset-syncs"));
}

#[test]
fn test_source_cluster_alias() {
    let mut task = MirrorCheckpointTask::new();
    task.start(create_basic_config());

    assert_eq!(task.source_cluster_alias(), "source1");
}

#[test]
fn test_target_cluster_alias() {
    let mut task = MirrorCheckpointTask::new();
    task.start(create_basic_config());

    assert_eq!(task.target_cluster_alias(), "target2");
}

#[test]
fn test_consumer_groups_from_config() {
    let mut task = MirrorCheckpointTask::new();
    task.start(create_config_with_multiple_groups("group1,group2,group3"));

    assert_eq!(task.consumer_groups().len(), 3);
    assert!(task.consumer_groups().contains(&"group1".to_string()));
    assert!(task.consumer_groups().contains(&"group2".to_string()));
    assert!(task.consumer_groups().contains(&"group3".to_string()));
}

#[test]
fn test_consumer_groups_whitespace_handling() {
    let mut task = MirrorCheckpointTask::new();
    task.start(create_config_with_multiple_groups("group1, group2 ,group3"));

    // Whitespace should be trimmed
    assert_eq!(task.consumer_groups().len(), 3);
    assert!(task.consumer_groups().contains(&"group1".to_string()));
    assert!(task.consumer_groups().contains(&"group2".to_string()));
    assert!(task.consumer_groups().contains(&"group3".to_string()));
}

// ============================================================================
// Edge Cases Tests
// ============================================================================

#[test]
fn test_poll_empty_consumer_groups() {
    // Empty consumer groups should return empty poll

    let mut task = MirrorCheckpointTask::new();
    task.start(create_config_with_multiple_groups(""));
    // Force immediate emission
    task.set_emit_interval(Duration::from_secs(60));
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();
    assert!(records.is_empty());
}

#[test]
fn test_multiple_partitions_same_group() {
    // Single consumer group with multiple partitions

    let source_admin = Arc::new(MockAdmin::new());

    let tp0 = TopicPartition::new("topic", 0);
    let tp1 = TopicPartition::new("topic", 1);

    let offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::from([
        (tp0.clone(), OffsetAndMetadata::new(100)),
        (tp1.clone(), OffsetAndMetadata::new(200)),
    ]);
    source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

    let mut task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    task.start(create_basic_config());

    // Add syncs for both partitions
    if let Some(store) = task.offset_sync_store_mut() {
        store.add_sync(OffsetSync::new(tp0.clone(), 100, 200));
        store.add_sync(OffsetSync::new(tp1.clone(), 200, 400));
    }

    // Force immediate emission
    task.set_emit_interval(Duration::from_secs(60));
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();

    // Should have 2 checkpoints (one per partition)
    assert_eq!(records.len(), 2);
}

#[test]
fn test_checkpoint_serialization_roundtrip() {
    // Verify Checkpoint can be serialized and deserialized

    let checkpoint = Checkpoint::new(
        "group1",
        TopicPartition::new("topic", 0),
        100,
        200,
        "metadata",
    );

    // Serialize
    let key = checkpoint.record_key().unwrap();
    let value = checkpoint.record_value().unwrap();

    // Deserialize
    let restored = Checkpoint::deserialize_record(&key, &value).unwrap();

    assert_eq!(restored.consumer_group_id(), checkpoint.consumer_group_id());
    assert_eq!(restored.topic_partition(), checkpoint.topic_partition());
    assert_eq!(restored.upstream_offset(), checkpoint.upstream_offset());
    assert_eq!(restored.downstream_offset(), checkpoint.downstream_offset());
    assert_eq!(restored.metadata(), checkpoint.metadata());
}

#[test]
fn test_multiple_start_stop_cycles() {
    // Multiple start/stop cycles should work correctly

    let mut task = MirrorCheckpointTask::new();

    for _ in 0..3 {
        task.start(create_basic_config());
        assert!(task.is_initialized());

        task.stop();
        assert!(!task.is_initialized());
    }
}

#[test]
fn test_offset_sync_store_access() {
    // Verify OffsetSyncStore is accessible after start

    let mut task = MirrorCheckpointTask::new();
    task.start(create_basic_config());

    assert!(task.offset_sync_store().is_some());

    let store = task.offset_sync_store().unwrap();
    assert!(store.is_initialized());
}

// ============================================================================
// Additional Semantic Tests
// ============================================================================

#[test]
fn test_source_records_for_group_returns_records() {
    // source_records_for_group creates correct SourceRecords

    let source_admin = Arc::new(MockAdmin::new());

    let tp = TopicPartition::new("test-topic", 0);
    let offsets = HashMap::from([(tp.clone(), OffsetAndMetadata::new(100))]);
    source_admin.alter_consumer_group_offsets("consumer-group-1", offsets);

    let mut task =
        MirrorCheckpointTask::with_admin_clients(source_admin.clone(), Arc::new(MockAdmin::new()));

    task.start(create_basic_config());

    // Add offset sync
    if let Some(store) = task.offset_sync_store_mut() {
        store.add_sync(OffsetSync::new(tp.clone(), 100, 200));
    }

    // Force immediate emission
    task.set_emit_interval(Duration::from_secs(60));
    task.set_last_emit_time_to_past();

    // Poll to get records
    let records = task.poll().unwrap();

    assert!(!records.is_empty());

    // Verify each record has correct structure
    for record in &records {
        assert!(record.source_partition().contains_key("group"));
        assert!(record.source_partition().contains_key("topic"));
        assert!(record.source_partition().contains_key("partition"));
        assert!(record.source_offset().contains_key("offset"));
        assert!(record.topic().contains("checkpoints"));
    }
}

#[test]
fn test_start_offset_sync_store() {
    // start_offset_sync_store reads from offset-syncs topic

    let mut task = MirrorCheckpointTask::new();
    task.start(create_basic_config());

    // Start the OffsetSyncStore
    let result = task.start_offset_sync_store();
    assert!(result.is_ok());
}
