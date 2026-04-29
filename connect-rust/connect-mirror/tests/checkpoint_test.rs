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

//! CheckpointTest - Tests for Checkpoint structure.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.CheckpointTest
//!
//! Test coverage:
//! - testSerde: Serialization/deserialization roundtrip validation
//! - Field validation: consumerGroupId, topicPartition, upstream/downstream offsets, metadata
//! - offsetAndMetadata: downstream offset and metadata pair extraction
//! - connectPartition: partition map for checkpoint identification

use common_trait::TopicPartition;
use connect_mirror_client::{
    Checkpoint, CONSUMER_GROUP_ID_KEY, DOWNSTREAM_OFFSET_KEY, METADATA_KEY, PARTITION_KEY,
    TOPIC_KEY, UPSTREAM_OFFSET_KEY,
};

// ============================================================================
// Java Correspondence: testSerde (exact parameters from Java test)
// ============================================================================

/// Test serde roundtrip for Checkpoint structure.
/// Corresponds to Java CheckpointTest.testSerde() exactly:
/// - Creates Checkpoint with ("group-1", "topic-1", 2, 3, 4, "metadata-1")
/// - Serializes key and value
/// - Deserializes back
/// - Verifies all fields match
#[test]
fn test_serde_java_correspondence() {
    // Exact parameters from Java test structure
    // Java: Checkpoint("group-1", new TopicPartition("topic-1", 2), 3, 4, "metadata-1")
    let checkpoint = Checkpoint::new(
        "group-1",
        TopicPartition::new("topic-1", 2),
        3, // upstreamOffset
        4, // downstreamOffset
        "metadata-1",
    );

    // Serialize key and value (corresponds to Java recordKey/recordValue)
    let key = checkpoint.record_key().expect("Failed to serialize key");
    let value = checkpoint
        .record_value()
        .expect("Failed to serialize value");

    // Deserialize back (corresponds to Java deserializeRecord)
    // Note: Java uses ConsumerRecord as carrier, Rust uses direct byte arrays
    let deserialized =
        Checkpoint::deserialize_record(&key, &value).expect("Failed to deserialize checkpoint");

    // Verify all fields match (Java assertions)
    assert_eq!(
        checkpoint.consumer_group_id(),
        deserialized.consumer_group_id(),
        "Failure on checkpoint consumerGroupId serde"
    );
    assert_eq!(
        checkpoint.topic_partition().topic(),
        deserialized.topic_partition().topic(),
        "Failure on checkpoint topic serde"
    );
    assert_eq!(
        checkpoint.topic_partition().partition(),
        deserialized.topic_partition().partition(),
        "Failure on checkpoint partition serde"
    );
    assert_eq!(
        checkpoint.upstream_offset(),
        deserialized.upstream_offset(),
        "Failure on checkpoint upstreamOffset serde"
    );
    assert_eq!(
        checkpoint.downstream_offset(),
        deserialized.downstream_offset(),
        "Failure on checkpoint downstreamOffset serde"
    );
    assert_eq!(
        checkpoint.metadata(),
        deserialized.metadata(),
        "Failure on checkpoint metadata serde"
    );
}

// ============================================================================
// Extended Serde Tests
// ============================================================================

/// Test serde with zero offsets (edge case).
#[test]
fn test_serde_zero_offsets() {
    let checkpoint = Checkpoint::new("group-zero", TopicPartition::new("topic-zero", 0), 0, 0, "");

    let key = checkpoint.record_key().expect("Failed to serialize key");
    let value = checkpoint
        .record_value()
        .expect("Failed to serialize value");

    let deserialized =
        Checkpoint::deserialize_record(&key, &value).expect("Failed to deserialize checkpoint");

    assert_eq!(checkpoint.upstream_offset(), deserialized.upstream_offset());
    assert_eq!(
        checkpoint.downstream_offset(),
        deserialized.downstream_offset()
    );
    assert_eq!(checkpoint.metadata(), deserialized.metadata());
}

/// Test serde with negative offsets (NO_OFFSET semantics).
/// Kafka uses -1 to represent NO_OFFSET (undefined offset).
#[test]
fn test_serde_negative_offsets() {
    let checkpoint = Checkpoint::new(
        "group-neg",
        TopicPartition::new("topic-neg", 5),
        -1, // NO_OFFSET semantics
        -1,
        "no-offset-meta",
    );

    let key = checkpoint.record_key().expect("Failed to serialize key");
    let value = checkpoint
        .record_value()
        .expect("Failed to serialize value");

    let deserialized =
        Checkpoint::deserialize_record(&key, &value).expect("Failed to deserialize checkpoint");

    assert_eq!(-1, deserialized.upstream_offset());
    assert_eq!(-1, deserialized.downstream_offset());
}

/// Test serde with mixed positive and negative offsets.
#[test]
fn test_serde_mixed_offsets() {
    let checkpoint = Checkpoint::new(
        "group-mixed",
        TopicPartition::new("topic-mixed", 3),
        100, // upstream valid
        -1,  // downstream NO_OFFSET
        "mixed-meta",
    );

    let key = checkpoint.record_key().expect("Failed to serialize key");
    let value = checkpoint
        .record_value()
        .expect("Failed to serialize value");

    let deserialized =
        Checkpoint::deserialize_record(&key, &value).expect("Failed to deserialize checkpoint");

    assert_eq!(100, deserialized.upstream_offset());
    assert_eq!(-1, deserialized.downstream_offset());
}

/// Test serde with large offset values (i64 boundary).
#[test]
fn test_serde_large_offsets() {
    let large_offset = i64::MAX / 2; // Large but not extreme
    let checkpoint = Checkpoint::new(
        "group-large",
        TopicPartition::new("topic-large", 99),
        large_offset,
        large_offset + 1,
        "large-meta",
    );

    let key = checkpoint.record_key().expect("Failed to serialize key");
    let value = checkpoint
        .record_value()
        .expect("Failed to serialize value");

    let deserialized =
        Checkpoint::deserialize_record(&key, &value).expect("Failed to deserialize checkpoint");

    assert_eq!(large_offset, deserialized.upstream_offset());
    assert_eq!(large_offset + 1, deserialized.downstream_offset());
}

/// Test serde with special characters in group/topic names.
#[test]
fn test_serde_special_characters() {
    let checkpoint = Checkpoint::new(
        "consumer-group-1",
        TopicPartition::new("my-topic-partition", 15),
        500,
        600,
        "metadata with spaces",
    );

    let key = checkpoint.record_key().expect("Failed to serialize key");
    let value = checkpoint
        .record_value()
        .expect("Failed to serialize value");

    let deserialized =
        Checkpoint::deserialize_record(&key, &value).expect("Failed to deserialize checkpoint");

    assert_eq!(
        checkpoint.consumer_group_id(),
        deserialized.consumer_group_id()
    );
    assert_eq!(
        checkpoint.topic_partition().topic(),
        deserialized.topic_partition().topic()
    );
    assert_eq!(checkpoint.metadata(), deserialized.metadata());
}

/// Test serde with empty strings.
#[test]
fn test_serde_empty_strings() {
    let checkpoint = Checkpoint::new("", TopicPartition::new("", 0), 0, 0, "");

    let key = checkpoint.record_key().expect("Failed to serialize key");
    let value = checkpoint
        .record_value()
        .expect("Failed to serialize value");

    let deserialized =
        Checkpoint::deserialize_record(&key, &value).expect("Failed to deserialize checkpoint");

    assert_eq!("", deserialized.consumer_group_id());
    assert_eq!("", deserialized.topic_partition().topic());
    assert_eq!("", deserialized.metadata());
}

/// Test multiple serde roundtrips preserve data.
#[test]
fn test_multiple_roundtrips() {
    let original = Checkpoint::new(
        "group-multi",
        TopicPartition::new("topic-multi", 7),
        1000,
        2000,
        "multi-meta",
    );

    // First roundtrip
    let key1 = original.record_key().expect("Failed to serialize key1");
    let value1 = original.record_value().expect("Failed to serialize value1");
    let first = Checkpoint::deserialize_record(&key1, &value1).expect("Failed first roundtrip");

    // Second roundtrip
    let key2 = first.record_key().expect("Failed to serialize key2");
    let value2 = first.record_value().expect("Failed to serialize value2");
    let second = Checkpoint::deserialize_record(&key2, &value2).expect("Failed second roundtrip");

    // All should be equal
    assert_eq!(original.consumer_group_id(), first.consumer_group_id());
    assert_eq!(original.consumer_group_id(), second.consumer_group_id());
    assert_eq!(original.upstream_offset(), first.upstream_offset());
    assert_eq!(original.upstream_offset(), second.upstream_offset());
}

// ============================================================================
// Key/Value Structure Tests
// ============================================================================

/// Test key schema structure: contains group, topic, partition.
#[test]
fn test_key_schema_structure() {
    let checkpoint = Checkpoint::new(
        "test-group",
        TopicPartition::new("test-topic", 10),
        50,
        60,
        "test-meta",
    );
    let key = checkpoint.record_key().expect("Failed to serialize key");

    // Key should be non-empty (schema + data)
    assert!(!key.is_empty(), "Key should not be empty");

    // Full roundtrip validates key structure
    let value = checkpoint
        .record_value()
        .expect("Failed to serialize value");
    let deserialized = Checkpoint::deserialize_record(&key, &value)
        .expect("Key deserialization should work with value");

    // Key data (group, topic, partition) is preserved in deserialized object
    assert_eq!(
        checkpoint.consumer_group_id(),
        deserialized.consumer_group_id()
    );
    assert_eq!(
        checkpoint.topic_partition().topic(),
        deserialized.topic_partition().topic()
    );
    assert_eq!(
        checkpoint.topic_partition().partition(),
        deserialized.topic_partition().partition()
    );
}

/// Test value schema structure: contains upstream/downstream offsets and metadata.
#[test]
fn test_value_schema_structure() {
    let checkpoint = Checkpoint::new(
        "value-group",
        TopicPartition::new("value-topic", 5),
        100,
        200,
        "value-metadata",
    );
    let value = checkpoint
        .record_value()
        .expect("Failed to serialize value");

    // Value should be non-empty (header + data)
    assert!(!value.is_empty(), "Value should not be empty");

    // First bytes should be header with version
    // VERSION_KEY (short = i16) takes 2 bytes plus schema overhead
    assert!(value.len() > 2, "Value should contain header and data");

    // Full roundtrip validates value structure
    let key = checkpoint.record_key().expect("Failed to serialize key");
    let deserialized =
        Checkpoint::deserialize_record(&key, &value).expect("Failed to deserialize checkpoint");

    // Value data (offsets, metadata) is preserved
    assert_eq!(checkpoint.upstream_offset(), deserialized.upstream_offset());
    assert_eq!(
        checkpoint.downstream_offset(),
        deserialized.downstream_offset()
    );
    assert_eq!(checkpoint.metadata(), deserialized.metadata());
}

// ============================================================================
// offsetAndMetadata Tests
// ============================================================================

/// Test offsetAndMetadata returns downstream offset and metadata pair.
/// Corresponds to Java: Checkpoint.offsetAndMetadata()
#[test]
fn test_offset_and_metadata_basic() {
    let checkpoint = Checkpoint::new(
        "group-a",
        TopicPartition::new("topic-a", 2),
        10,
        20,
        "meta-a",
    );

    let offset_and_meta = checkpoint.offset_and_metadata();
    assert_eq!(
        offset_and_meta.offset(),
        20,
        "Should return downstream offset"
    );
    assert_eq!(
        offset_and_meta.metadata(),
        "meta-a",
        "Should return metadata"
    );
}

/// Test offsetAndMetadata with empty metadata.
#[test]
fn test_offset_and_metadata_empty_meta() {
    let checkpoint = Checkpoint::new(
        "group-empty",
        TopicPartition::new("topic-empty", 1),
        100,
        200,
        "",
    );

    let offset_and_meta = checkpoint.offset_and_metadata();
    assert_eq!(offset_and_meta.offset(), 200);
    assert_eq!(offset_and_meta.metadata(), "");
}

/// Test offsetAndMetadata with NO_OFFSET (-1).
#[test]
fn test_offset_and_metadata_no_offset() {
    let checkpoint = Checkpoint::new(
        "group-no",
        TopicPartition::new("topic-no", 0),
        0,
        -1, // downstream NO_OFFSET
        "no-downstream",
    );

    let offset_and_meta = checkpoint.offset_and_metadata();
    assert_eq!(-1, offset_and_meta.offset());
    assert_eq!("no-downstream", offset_and_meta.metadata());
}

// ============================================================================
// connectPartition Tests
// ============================================================================

/// Test connectPartition returns correct keys and values.
/// Corresponds to Java: Checkpoint.connectPartition()
#[test]
fn test_connect_partition_keys() {
    let checkpoint = Checkpoint::new(
        "group-partition",
        TopicPartition::new("topic-partition", 25),
        500,
        600,
        "partition-meta",
    );

    let partition = checkpoint.connect_partition();

    // Verify partition contains all required keys
    assert!(partition.contains_key(CONSUMER_GROUP_ID_KEY));
    assert!(partition.contains_key(TOPIC_KEY));
    assert!(partition.contains_key(PARTITION_KEY));

    // Verify values match checkpoint fields
    assert_eq!(
        partition.get(CONSUMER_GROUP_ID_KEY),
        Some(&"group-partition".to_string())
    );
    assert_eq!(
        partition.get(TOPIC_KEY),
        Some(&"topic-partition".to_string())
    );
    assert_eq!(partition.get(PARTITION_KEY), Some(&"25".to_string()));

    // Verify partition has exactly 3 entries
    assert_eq!(partition.len(), 3);
}

/// Test connectPartition with various group/topic combinations.
#[test]
fn test_connect_partition_various_combinations() {
    // Test case 1: normal names
    let checkpoint1 = Checkpoint::new(
        "my-consumer-group",
        TopicPartition::new("my-topic", 0),
        0,
        0,
        "",
    );
    let partition1 = checkpoint1.connect_partition();
    assert_eq!(
        partition1.get(CONSUMER_GROUP_ID_KEY),
        Some(&"my-consumer-group".to_string())
    );
    assert_eq!(partition1.get(TOPIC_KEY), Some(&"my-topic".to_string()));
    assert_eq!(partition1.get(PARTITION_KEY), Some(&"0".to_string()));

    // Test case 2: empty strings
    let checkpoint2 = Checkpoint::new("", TopicPartition::new("", 0), 0, 0, "");
    let partition2 = checkpoint2.connect_partition();
    assert_eq!(partition2.get(CONSUMER_GROUP_ID_KEY), Some(&"".to_string()));
    assert_eq!(partition2.get(TOPIC_KEY), Some(&"".to_string()));
    assert_eq!(partition2.get(PARTITION_KEY), Some(&"0".to_string()));

    // Test case 3: large partition number
    let checkpoint3 = Checkpoint::new(
        "group-large-p",
        TopicPartition::new("topic-large-p", 9999),
        0,
        0,
        "",
    );
    let partition3 = checkpoint3.connect_partition();
    assert_eq!(partition3.get(PARTITION_KEY), Some(&"9999".to_string()));
}

/// Test unwrapGroup extracts group from connectPartition.
/// Corresponds to Java: Checkpoint.unwrapGroup(Map<String, ?> connectPartition)
#[test]
fn test_unwrap_group_basic() {
    let checkpoint = Checkpoint::new(
        "group-unwrap",
        TopicPartition::new("topic-unwrap", 5),
        10,
        20,
        "unwrap-meta",
    );

    let partition = checkpoint.connect_partition();
    let unwrapped_group = Checkpoint::unwrap_group(&partition);

    assert_eq!(unwrapped_group, "group-unwrap");
}

/// Test unwrapGroup returns empty string when key missing.
#[test]
fn test_unwrap_group_missing_key() {
    let empty_partition: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let unwrapped = Checkpoint::unwrap_group(&empty_partition);
    assert_eq!(unwrapped, "");
}

// ============================================================================
// Display Tests
// ============================================================================

/// Test Display implementation matches Java toString format.
#[test]
fn test_display_format() {
    let checkpoint = Checkpoint::new(
        "display-group",
        TopicPartition::new("display-topic", 10),
        1000,
        2000,
        "display-metadata",
    );
    let display_str = checkpoint.to_string();

    // Verify format matches Java: Checkpoint{consumerGroupId=..., topicPartition=..., upstreamOffset=..., downstreamOffset=..., metadata=...}
    assert!(
        display_str.contains("Checkpoint"),
        "Should contain Checkpoint"
    );
    assert!(display_str.contains("consumerGroupId=display-group"));
    assert!(display_str.contains("upstreamOffset=1000"));
    assert!(display_str.contains("downstreamOffset=2000"));
    assert!(display_str.contains("metadata=display-metadata"));
}

/// Test Display format with NO_OFFSET values.
#[test]
fn test_display_format_no_offset() {
    let checkpoint = Checkpoint::new(
        "display-no",
        TopicPartition::new("display-topic-no", 0),
        -1,
        -1,
        "",
    );
    let display_str = checkpoint.to_string();

    assert!(display_str.contains("upstreamOffset=-1"));
    assert!(display_str.contains("downstreamOffset=-1"));
    assert!(display_str.contains("metadata="));
}

// ============================================================================
// Getter Tests
// ============================================================================

/// Test all getter methods return correct values.
#[test]
fn test_getter_methods() {
    let checkpoint = Checkpoint::new(
        "getter-group",
        TopicPartition::new("getter-topic", 42),
        12345,
        67890,
        "getter-metadata",
    );

    assert_eq!(checkpoint.consumer_group_id(), "getter-group");
    assert_eq!(checkpoint.topic_partition().topic(), "getter-topic");
    assert_eq!(checkpoint.topic_partition().partition(), 42);
    assert_eq!(checkpoint.upstream_offset(), 12345);
    assert_eq!(checkpoint.downstream_offset(), 67890);
    assert_eq!(checkpoint.metadata(), "getter-metadata");
}

/// Test getter methods with edge case values.
#[test]
fn test_getter_edge_cases() {
    // Zero partition
    let checkpoint1 = Checkpoint::new(
        "group-zero-p",
        TopicPartition::new("topic-zero-p", 0),
        0,
        0,
        "",
    );
    assert_eq!(checkpoint1.topic_partition().partition(), 0);

    // Large partition
    let checkpoint2 = Checkpoint::new(
        "group-large-p",
        TopicPartition::new("topic-large-p", i32::MAX),
        i64::MAX,
        i64::MAX,
        "",
    );
    assert_eq!(checkpoint2.topic_partition().partition(), i32::MAX);
    assert_eq!(checkpoint2.upstream_offset(), i64::MAX);
    assert_eq!(checkpoint2.downstream_offset(), i64::MAX);

    // Negative offsets
    let checkpoint3 = Checkpoint::new(
        "group-neg",
        TopicPartition::new("topic-neg", 1),
        -1,
        -1,
        "neg-meta",
    );
    assert_eq!(checkpoint3.upstream_offset(), -1);
    assert_eq!(checkpoint3.downstream_offset(), -1);
}

// ============================================================================
// Equality Tests
// ============================================================================

/// Test Checkpoint equality based on all fields.
#[test]
fn test_equality() {
    let checkpoint1 = Checkpoint::new(
        "eq-group",
        TopicPartition::new("eq-topic", 5),
        100,
        200,
        "eq-meta",
    );
    let checkpoint2 = Checkpoint::new(
        "eq-group",
        TopicPartition::new("eq-topic", 5),
        100,
        200,
        "eq-meta",
    );
    let checkpoint3 = Checkpoint::new(
        "eq-group",
        TopicPartition::new("eq-topic", 5),
        100,
        200,
        "different-meta",
    );
    let checkpoint4 = Checkpoint::new(
        "different-group",
        TopicPartition::new("eq-topic", 5),
        100,
        200,
        "eq-meta",
    );

    // Same values should be equal
    assert_eq!(checkpoint1, checkpoint2);

    // Different metadata should not be equal
    assert_ne!(checkpoint1, checkpoint3);

    // Different group should not be equal
    assert_ne!(checkpoint1, checkpoint4);
}

/// Test Checkpoint clone preserves all fields.
#[test]
fn test_clone() {
    let checkpoint = Checkpoint::new(
        "clone-group",
        TopicPartition::new("clone-topic", 10),
        500,
        600,
        "clone-meta",
    );
    let cloned = checkpoint.clone();

    assert_eq!(checkpoint.consumer_group_id(), cloned.consumer_group_id());
    assert_eq!(
        checkpoint.topic_partition().topic(),
        cloned.topic_partition().topic()
    );
    assert_eq!(
        checkpoint.topic_partition().partition(),
        cloned.topic_partition().partition()
    );
    assert_eq!(checkpoint.upstream_offset(), cloned.upstream_offset());
    assert_eq!(checkpoint.downstream_offset(), cloned.downstream_offset());
    assert_eq!(checkpoint.metadata(), cloned.metadata());
}

// ============================================================================
// Edge Cases Tests
// ============================================================================

/// Test serde with Unicode characters in group/topic names.
#[test]
fn test_serde_unicode_names() {
    let checkpoint = Checkpoint::new(
        "グループ",                     // Japanese
        TopicPartition::new("话题", 5), // Chinese
        100,
        200,
        "unicode-meta",
    );

    let key = checkpoint.record_key().expect("Failed to serialize key");
    let value = checkpoint
        .record_value()
        .expect("Failed to serialize value");

    let deserialized =
        Checkpoint::deserialize_record(&key, &value).expect("Failed to deserialize checkpoint");

    assert_eq!(
        checkpoint.consumer_group_id(),
        deserialized.consumer_group_id()
    );
    assert_eq!(
        checkpoint.topic_partition().topic(),
        deserialized.topic_partition().topic()
    );
}

/// Test serde with very long names.
#[test]
fn test_serde_long_names() {
    let long_name = "a".repeat(1000);
    let checkpoint = Checkpoint::new(
        &long_name,
        TopicPartition::new(&long_name, 100),
        1000,
        2000,
        &long_name,
    );

    let key = checkpoint.record_key().expect("Failed to serialize key");
    let value = checkpoint
        .record_value()
        .expect("Failed to serialize value");

    let deserialized =
        Checkpoint::deserialize_record(&key, &value).expect("Failed to deserialize checkpoint");

    assert_eq!(
        checkpoint.consumer_group_id(),
        deserialized.consumer_group_id()
    );
    assert_eq!(
        checkpoint.topic_partition().topic(),
        deserialized.topic_partition().topic()
    );
    assert_eq!(checkpoint.metadata(), deserialized.metadata());
}

/// Test checkpoint creation with all zero values.
#[test]
fn test_checkpoint_all_zeros() {
    let checkpoint = Checkpoint::new("", TopicPartition::new("", 0), 0, 0, "");

    assert_eq!(checkpoint.consumer_group_id(), "");
    assert_eq!(checkpoint.topic_partition().topic(), "");
    assert_eq!(checkpoint.topic_partition().partition(), 0);
    assert_eq!(checkpoint.upstream_offset(), 0);
    assert_eq!(checkpoint.downstream_offset(), 0);
    assert_eq!(checkpoint.metadata(), "");

    // Serde should still work
    let key = checkpoint.record_key().expect("Key serialization failed");
    let value = checkpoint
        .record_value()
        .expect("Value serialization failed");
    let deserialized =
        Checkpoint::deserialize_record(&key, &value).expect("Deserialization failed");

    assert_eq!(
        checkpoint.consumer_group_id(),
        deserialized.consumer_group_id()
    );
}

/// Test checkpoint with maximum i32 partition value.
#[test]
fn test_checkpoint_max_partition() {
    let checkpoint = Checkpoint::new(
        "max-part-group",
        TopicPartition::new("max-part-topic", i32::MAX),
        100,
        200,
        "max-part-meta",
    );

    assert_eq!(checkpoint.topic_partition().partition(), i32::MAX);

    let key = checkpoint.record_key().expect("Failed to serialize key");
    let value = checkpoint
        .record_value()
        .expect("Failed to serialize value");
    let deserialized = Checkpoint::deserialize_record(&key, &value).expect("Failed to deserialize");

    assert_eq!(
        checkpoint.topic_partition().partition(),
        deserialized.topic_partition().partition()
    );
}
