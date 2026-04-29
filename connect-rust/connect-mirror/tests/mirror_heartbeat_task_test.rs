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

//! MirrorHeartbeatTaskTest - Tests for MirrorHeartbeatTask.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorHeartbeatTaskTest
//!
//! Test coverage:
//! - testPollCreatesRecords: Java core test - poll creates heartbeat SourceRecord
//! - Heartbeat record structure (sourcePartition, sourceOffset)
//! - Lifecycle tests (start/stop/initialize)
//! - Poll behavior (interval elapsed, stopping, not initialized)
//! - Edge cases (disabled heartbeats, zero interval)

use std::collections::HashMap;
use std::time::Duration;

use common_trait::metrics::PluginMetrics;
use connect_api::connector::{ConnectRecord, Task};
use connect_api::errors::ConnectError;
use connect_api::source::{SourceRecord, SourceTask as SourceTaskTrait, SourceTaskContext};
use connect_api::storage::OffsetStorageReader;
use connect_mirror::config::{
    NAME_CONFIG, SOURCE_CLUSTER_ALIAS_CONFIG, TARGET_CLUSTER_ALIAS_CONFIG,
};
use connect_mirror::heartbeat::{
    MirrorHeartbeatTask, EMIT_HEARTBEATS_ENABLED_CONFIG, EMIT_HEARTBEATS_INTERVAL_SECONDS_CONFIG,
};
use connect_mirror_client::Heartbeat;
use kafka_clients_mock::RecordMetadata;
use serde_json::json;

// ============================================================================
// Mock Implementations for Testing
// ============================================================================

/// Mock OffsetStorageReader for testing.
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
                key.get("sourceClusterAlias") == partition.get("sourceClusterAlias")
                    && key.get("targetClusterAlias") == partition.get("targetClusterAlias")
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

/// Creates a basic test configuration for MirrorHeartbeatTask.
/// Corresponds to Java MirrorHeartbeatTaskTest setup.
fn create_test_config() -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert(
        SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
        "backup".to_string(),
    );
    props.insert(
        TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
        "primary".to_string(),
    );
    props.insert(NAME_CONFIG.to_string(), "mirror-heartbeat-task".to_string());
    props
}

/// Creates a configuration with custom heartbeat interval.
fn create_config_with_interval(interval: u64) -> HashMap<String, String> {
    let mut props = create_test_config();
    props.insert(
        EMIT_HEARTBEATS_INTERVAL_SECONDS_CONFIG.to_string(),
        interval.to_string(),
    );
    props
}

/// Creates a configuration with heartbeats disabled.
fn create_config_with_heartbeats_disabled() -> HashMap<String, String> {
    let mut props = create_test_config();
    props.insert(
        EMIT_HEARTBEATS_ENABLED_CONFIG.to_string(),
        "false".to_string(),
    );
    props
}

/// Creates a configuration with zero interval.
fn create_config_with_zero_interval() -> HashMap<String, String> {
    let mut props = create_test_config();
    props.insert(
        EMIT_HEARTBEATS_INTERVAL_SECONDS_CONFIG.to_string(),
        "0".to_string(),
    );
    props
}

// ============================================================================
// testPollCreatesRecords - Java Core Test
// ============================================================================

/// Java: testPollCreatesRecords
/// Tests that poll() creates a SourceRecord representing a heartbeat.
///
/// Java semantics:
/// 1. poll() returns exactly one SourceRecord
/// 2. sourcePartition contains source.cluster.alias and target.cluster.alias
/// 3. Heartbeat record is properly attributed to source/target clusters
#[test]
fn test_poll_creates_records_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_test_config();
    task.start(props);

    // Java: heartbeatTask.poll()
    // Force interval elapsed by setting last_emit_time to past
    // This simulates the condition where interval has elapsed
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();

    // Java assertion: exactly one SourceRecord
    assert_eq!(records.len(), 1, "Expected exactly one heartbeat record");

    let record = &records[0];

    // Java: verify sourcePartition contains correct aliases
    let source_partition = record.source_partition();
    assert_eq!(
        source_partition.get("sourceClusterAlias"),
        Some(&json!("backup")),
        "sourcePartition must contain source.cluster.alias"
    );
    assert_eq!(
        source_partition.get("targetClusterAlias"),
        Some(&json!("primary")),
        "sourcePartition must contain target.cluster.alias"
    );

    // Java: verify topic is heartbeats topic
    assert_eq!(record.topic(), "backup.heartbeats");

    // Java: verify partition is 0 (heartbeats topic has single partition)
    assert_eq!(record.kafka_partition(), Some(0));

    // Java: verify sourceOffset contains timestamp
    let source_offset = record.source_offset();
    assert!(
        source_offset.contains_key("timestamp"),
        "sourceOffset must contain timestamp"
    );
}

/// Java: testPollCreatesRecords - extended verification
/// Tests heartbeat record key/value serialization.
#[test]
fn test_poll_creates_records_key_value_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_test_config();
    task.start(props);
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();
    assert_eq!(records.len(), 1);

    let record = &records[0];

    // Heartbeat key is serialized bytes (sourceClusterAlias + targetClusterAlias)
    // Heartbeat value is serialized bytes (timestamp)
    // Both are JSON arrays of bytes in Rust implementation
    assert!(record.key().is_some(), "Heartbeat record must have key");
    assert!(
        !record.value().is_null(),
        "Heartbeat record must have value"
    );
}

/// Java: testPollCreatesRecords - multiple poll calls
/// Tests that multiple poll calls produce different timestamps.
#[test]
fn test_poll_creates_records_multiple_calls_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_config_with_interval(1);
    task.start(props);

    // First poll - interval elapsed
    task.set_last_emit_time_to_past();
    let records1 = task.poll().unwrap();
    assert_eq!(records1.len(), 1);

    let ts1 = records1[0]
        .source_offset()
        .get("timestamp")
        .and_then(|v| v.as_i64())
        .unwrap();

    // Wait for interval to elapse
    std::thread::sleep(Duration::from_millis(10));

    // Force second poll
    task.set_last_emit_time_to_past();
    let records2 = task.poll().unwrap();
    assert_eq!(records2.len(), 1);

    let ts2 = records2[0]
        .source_offset()
        .get("timestamp")
        .and_then(|v| v.as_i64())
        .unwrap();

    // Timestamps should be different (or at least second >= first)
    assert!(ts2 >= ts1, "Timestamp should be monotonically increasing");
}

// ============================================================================
// Lifecycle Tests - Start/Stop/Initialize
// ============================================================================

/// Tests task initialization with context.
#[test]
fn test_initialize_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let context = Box::new(MockSourceTaskContext::new());

    task.initialize(context);

    assert!(
        task.has_context(),
        "Task should have context after initialize"
    );
}

/// Tests start lifecycle with configuration.
#[test]
fn test_start_lifecycle_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_test_config();

    task.start(props);

    // Verify configuration is loaded correctly
    assert_eq!(task.source_cluster_alias(), "backup");
    assert_eq!(task.target_cluster_alias(), "primary");
    assert_eq!(task.heartbeats_topic(), "backup.heartbeats");
    assert!(
        task.is_initialized(),
        "Task should be initialized after start"
    );
}

/// Tests stop lifecycle clears state.
#[test]
fn test_stop_lifecycle_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_test_config();
    task.start(props);

    task.stop();

    // Verify state is cleared
    assert!(task.is_stopping(), "Task should be stopping after stop");
    assert!(
        !task.is_initialized(),
        "Task should not be initialized after stop"
    );
}

/// Tests full lifecycle: initialize -> start -> poll -> stop.
#[test]
fn test_full_lifecycle_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();

    // Initialize first
    let context = Box::new(MockSourceTaskContext::new());
    task.initialize(context);

    // Then start
    let props = create_test_config();
    task.start(props);

    // Poll should work
    task.set_last_emit_time_to_past();
    let records = task.poll().unwrap();
    assert_eq!(records.len(), 1);

    // Stop clears state
    task.stop();

    // Poll after stop returns empty
    let records = task.poll().unwrap();
    assert!(records.is_empty(), "Poll after stop should return empty");
}

// ============================================================================
// Poll Behavior Tests
// ============================================================================

/// Tests poll returns empty when interval not elapsed.
#[test]
fn test_poll_interval_not_elapsed_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_config_with_interval(10);
    task.start(props);

    // Poll immediately - interval not elapsed
    let records = task.poll().unwrap();
    assert!(
        records.is_empty(),
        "Poll should return empty when interval not elapsed"
    );
}

/// Tests poll returns record when interval elapsed.
#[test]
fn test_poll_interval_elapsed_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_config_with_interval(1);
    task.start(props);

    // Force interval elapsed
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();
    assert_eq!(
        records.len(),
        1,
        "Poll should return record when interval elapsed"
    );
}

/// Tests poll returns empty when task is stopping.
#[test]
fn test_poll_when_stopping_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_test_config();
    task.start(props);
    task.stop();

    let records = task.poll().unwrap();
    assert!(records.is_empty(), "Poll should return empty when stopping");
}

/// Tests poll returns empty when task not initialized.
#[test]
fn test_poll_when_not_initialized_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();

    // Not started - not initialized
    let records = task.poll().unwrap();
    assert!(
        records.is_empty(),
        "Poll should return empty when not initialized"
    );
}

// ============================================================================
// Disabled Heartbeats Tests
// ============================================================================

/// Tests poll returns empty when heartbeats disabled.
#[test]
fn test_poll_heartbeats_disabled_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_config_with_heartbeats_disabled();
    task.start(props);

    // emit.heartbeats.enabled=false means emit_interval is Duration::ZERO
    assert_eq!(task.emit_interval(), Duration::ZERO);

    // Poll always returns empty when disabled
    let records = task.poll().unwrap();
    assert!(
        records.is_empty(),
        "Poll should return empty when heartbeats disabled"
    );
}

/// Tests poll returns empty with zero interval.
#[test]
fn test_poll_zero_interval_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_config_with_zero_interval();
    task.start(props);

    // Zero interval is treated as disabled
    assert_eq!(task.emit_interval(), Duration::ZERO);

    let records = task.poll().unwrap();
    assert!(
        records.is_empty(),
        "Poll should return empty with zero interval"
    );
}

// ============================================================================
// Heartbeat Record Structure Tests
// ============================================================================

/// Tests heartbeat record source partition structure.
#[test]
fn test_heartbeat_record_source_partition_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_test_config();
    task.start(props);
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();
    let record = &records[0];

    let source_partition = record.source_partition();

    // Java: sourcePartition must have exactly 2 keys
    assert_eq!(source_partition.len(), 2);

    // Java: keys are sourceClusterAlias and targetClusterAlias
    assert!(source_partition.contains_key("sourceClusterAlias"));
    assert!(source_partition.contains_key("targetClusterAlias"));
}

/// Tests heartbeat record source offset structure.
#[test]
fn test_heartbeat_record_source_offset_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_test_config();
    task.start(props);
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();
    let record = &records[0];

    let source_offset = record.source_offset();

    // Java: sourceOffset must have exactly 1 key (timestamp)
    assert_eq!(source_offset.len(), 1);
    assert!(source_offset.contains_key("timestamp"));

    // Timestamp must be a valid i64
    let timestamp = source_offset.get("timestamp").and_then(|v| v.as_i64());
    assert!(timestamp.is_some());
    assert!(timestamp.unwrap() > 0, "Timestamp should be positive");
}

/// Tests heartbeat record topic name format.
#[test]
fn test_heartbeat_record_topic_format_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_test_config();
    task.start(props);

    // Java: heartbeats topic format is sourceAlias.heartbeats
    assert_eq!(task.heartbeats_topic(), "backup.heartbeats");

    task.set_last_emit_time_to_past();
    let records = task.poll().unwrap();
    assert_eq!(records[0].topic(), "backup.heartbeats");
}

/// Tests heartbeat record partition is always 0.
#[test]
fn test_heartbeat_record_partition_zero_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_test_config();
    task.start(props);
    task.set_last_emit_time_to_past();

    let records = task.poll().unwrap();
    let record = &records[0];

    // Java: heartbeats topic has single partition (partition 0)
    assert_eq!(record.kafka_partition(), Some(0));
}

// ============================================================================
// Commit Tests
// ============================================================================

/// Tests commit is a no-operation (Java semantics).
#[test]
fn test_commit_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();

    // Java: commit() is a no-op
    let result = task.commit();
    assert!(result.is_ok(), "Commit should always succeed");
}

/// Tests commitRecord is a no-operation (Java semantics).
#[test]
fn test_commit_record_java_semantic() {
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

    // Java: commitRecord() is a no-op
    let result = task.commit_record(&record, &metadata);
    assert!(result.is_ok(), "CommitRecord should always succeed");
}

// ============================================================================
// Version Test
// ============================================================================

/// Tests task version constant.
#[test]
fn test_version_java_semantic() {
    let task = MirrorHeartbeatTask::new();

    // Java: version() returns version string
    assert_eq!(task.version(), "1.0.0");
}

// ============================================================================
// Edge Cases Tests
// ============================================================================

/// Tests task with missing source alias uses default.
#[test]
fn test_missing_source_alias_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let mut props = HashMap::new();
    props.insert(
        TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
        "primary".to_string(),
    );

    task.start(props);

    // Should use default source alias
    assert_eq!(task.source_cluster_alias(), "source");
}

/// Tests task with missing target alias uses default.
#[test]
fn test_missing_target_alias_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let mut props = HashMap::new();
    props.insert(
        SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
        "backup".to_string(),
    );

    task.start(props);

    // Should use default target alias
    assert_eq!(task.target_cluster_alias(), "target");
}

/// Tests heartbeat creation with correct aliases.
#[test]
fn test_create_heartbeat_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_test_config();
    task.start(props);

    let heartbeat = task.create_heartbeat();

    // Java: Heartbeat contains source and target cluster aliases
    assert_eq!(heartbeat.source_cluster_alias(), "backup");
    assert_eq!(heartbeat.target_cluster_alias(), "primary");

    // Timestamp should be current time
    let timestamp = heartbeat.timestamp();
    assert!(timestamp > 0, "Heartbeat timestamp should be positive");
}

/// Tests heartbeat record creation from Heartbeat struct.
#[test]
fn test_heartbeat_record_from_struct_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_test_config();
    task.start(props);

    // Create heartbeat with known timestamp
    let heartbeat = Heartbeat::new("backup", "primary", 1234567890);
    let record = task.heartbeat_record(&heartbeat).unwrap();

    // Verify record matches heartbeat
    assert_eq!(record.topic(), "backup.heartbeats");
    assert_eq!(record.kafka_partition(), Some(0));

    let source_partition = record.source_partition();
    assert_eq!(
        source_partition.get("sourceClusterAlias"),
        Some(&json!("backup"))
    );
    assert_eq!(
        source_partition.get("targetClusterAlias"),
        Some(&json!("primary"))
    );

    // Offset timestamp should match heartbeat timestamp
    let source_offset = record.source_offset();
    assert_eq!(source_offset.get("timestamp"), Some(&json!(1234567890)));
}

/// Tests custom heartbeat interval configuration.
#[test]
fn test_custom_interval_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_config_with_interval(5);
    task.start(props);

    assert_eq!(task.emit_interval(), Duration::from_secs(5));
}

/// Tests task debug format contains key fields.
#[test]
fn test_debug_format_java_semantic() {
    let task = MirrorHeartbeatTask::new();
    let debug_str = format!("{:?}", task);

    // Debug output should contain key fields
    assert!(debug_str.contains("source_cluster_alias"));
    assert!(debug_str.contains("target_cluster_alias"));
    assert!(debug_str.contains("heartbeats_topic"));
}

// ============================================================================
// Heartbeat Serialization Tests
// ============================================================================

/// Tests Heartbeat key serialization format.
#[test]
fn test_heartbeat_key_serialization_java_semantic() {
    let heartbeat = Heartbeat::new("backup", "primary", 1234567890);

    let key_bytes = heartbeat.record_key().unwrap();

    // Key should be non-empty bytes
    assert!(!key_bytes.is_empty(), "Heartbeat key should be non-empty");
}

/// Tests Heartbeat value serialization format.
#[test]
fn test_heartbeat_value_serialization_java_semantic() {
    let heartbeat = Heartbeat::new("backup", "primary", 1234567890);

    let value_bytes = heartbeat.record_value().unwrap();

    // Value should be non-empty bytes
    assert!(
        !value_bytes.is_empty(),
        "Heartbeat value should be non-empty"
    );
}

/// Tests Heartbeat serde roundtrip.
#[test]
fn test_heartbeat_serde_roundtrip_java_semantic() {
    let heartbeat = Heartbeat::new("backup", "primary", 1234567890);

    let key = heartbeat.record_key().unwrap();
    let value = heartbeat.record_value().unwrap();

    // Deserialize back
    let deserialized = Heartbeat::deserialize_record(&key, &value).unwrap();

    assert_eq!(deserialized.source_cluster_alias(), "backup");
    assert_eq!(deserialized.target_cluster_alias(), "primary");
    assert_eq!(deserialized.timestamp(), 1234567890);
}

// ============================================================================
// Multiple Polls with Interval Behavior
// ============================================================================

/// Tests that poll respects interval across multiple calls.
#[test]
fn test_poll_respects_interval_multiple_calls_java_semantic() {
    let mut task = MirrorHeartbeatTask::new();
    let props = create_config_with_interval(1);
    task.start(props);

    // First poll - interval elapsed
    task.set_last_emit_time_to_past();
    let records1 = task.poll().unwrap();
    assert_eq!(records1.len(), 1);

    // Immediate second poll - interval not elapsed
    let records2 = task.poll().unwrap();
    assert!(
        records2.is_empty(),
        "Second immediate poll should return empty"
    );

    // Wait and force third poll
    std::thread::sleep(Duration::from_millis(10));
    task.set_last_emit_time_to_past();
    let records3 = task.poll().unwrap();
    assert_eq!(records3.len(), 1);
}
