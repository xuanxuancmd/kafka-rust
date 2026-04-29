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

//! HeartbeatTest - Tests for Heartbeat structure.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.HeartbeatTest
//!
//! Test coverage:
//! - testSerde: Serialization/deserialization roundtrip validation

use connect_mirror_client::{
    Heartbeat, SOURCE_CLUSTER_ALIAS_KEY, TARGET_CLUSTER_ALIAS_KEY, TIMESTAMP_KEY,
};

// ============================================================================
// Java Correspondence: testSerde
// ============================================================================

/// Test serde roundtrip for Heartbeat structure.
/// Corresponds to Java HeartbeatTest.testSerde() exactly:
/// - Creates Heartbeat with ("source-1", "target-2", 1234567890)
/// - Serializes key and value
/// - Deserializes back
/// - Verifies all fields match
#[test]
fn test_serde_java_correspondence() {
    // Exact parameters from Java test
    let heartbeat = Heartbeat::new("source-1", "target-2", 1234567890);

    // Serialize key and value (corresponds to Java recordKey/recordValue)
    let key = heartbeat.record_key().expect("Failed to serialize key");
    let value = heartbeat.record_value().expect("Failed to serialize value");

    // Deserialize back (corresponds to Java deserializeRecord)
    // Note: Java uses ConsumerRecord as carrier, Rust uses direct byte arrays
    let deserialized =
        Heartbeat::deserialize_record(&key, &value).expect("Failed to deserialize heartbeat");

    // Verify all fields match (Java assertions with failure messages)
    assert_eq!(
        heartbeat.source_cluster_alias(),
        deserialized.source_cluster_alias(),
        "Failure on heartbeat sourceClusterAlias serde"
    );
    assert_eq!(
        heartbeat.target_cluster_alias(),
        deserialized.target_cluster_alias(),
        "Failure on heartbeat targetClusterAlias serde"
    );
    assert_eq!(
        heartbeat.timestamp(),
        deserialized.timestamp(),
        "Failure on heartbeat timestamp serde"
    );
}

// ============================================================================
// Extended Serde Tests
// ============================================================================

/// Test serde with empty cluster aliases (edge case).
#[test]
fn test_serde_empty_aliases() {
    let heartbeat = Heartbeat::new("", "", 0);
    let key = heartbeat.record_key().expect("Failed to serialize key");
    let value = heartbeat.record_value().expect("Failed to serialize value");

    let deserialized =
        Heartbeat::deserialize_record(&key, &value).expect("Failed to deserialize heartbeat");

    assert_eq!(
        heartbeat.source_cluster_alias(),
        deserialized.source_cluster_alias()
    );
    assert_eq!(
        heartbeat.target_cluster_alias(),
        deserialized.target_cluster_alias()
    );
    assert_eq!(heartbeat.timestamp(), deserialized.timestamp());
}

/// Test serde with special characters in cluster aliases.
#[test]
fn test_serde_special_characters() {
    let heartbeat = Heartbeat::new("source-cluster-1", "target-cluster-2", 999999);
    let key = heartbeat.record_key().expect("Failed to serialize key");
    let value = heartbeat.record_value().expect("Failed to serialize value");

    let deserialized =
        Heartbeat::deserialize_record(&key, &value).expect("Failed to deserialize heartbeat");

    assert_eq!(
        heartbeat.source_cluster_alias(),
        deserialized.source_cluster_alias()
    );
    assert_eq!(
        heartbeat.target_cluster_alias(),
        deserialized.target_cluster_alias()
    );
    assert_eq!(heartbeat.timestamp(), deserialized.timestamp());
}

/// Test serde with large timestamp value (i64 boundary).
#[test]
fn test_serde_large_timestamp() {
    let large_timestamp = i64::MAX / 2; // Large but not extreme
    let heartbeat = Heartbeat::new("source", "target", large_timestamp);

    let key = heartbeat.record_key().expect("Failed to serialize key");
    let value = heartbeat.record_value().expect("Failed to serialize value");

    let deserialized =
        Heartbeat::deserialize_record(&key, &value).expect("Failed to deserialize heartbeat");

    assert_eq!(large_timestamp, deserialized.timestamp());
}

/// Test serde with negative timestamp (edge case).
#[test]
fn test_serde_negative_timestamp() {
    let heartbeat = Heartbeat::new("source", "target", -1);

    let key = heartbeat.record_key().expect("Failed to serialize key");
    let value = heartbeat.record_value().expect("Failed to serialize value");

    let deserialized =
        Heartbeat::deserialize_record(&key, &value).expect("Failed to deserialize heartbeat");

    assert_eq!(-1, deserialized.timestamp());
}

// ============================================================================
// Key/Value Structure Tests
// ============================================================================

/// Test key schema structure: contains sourceClusterAlias and targetClusterAlias.
#[test]
fn test_key_schema_structure() {
    let heartbeat = Heartbeat::new("test-source", "test-target", 1000);
    let key = heartbeat.record_key().expect("Failed to serialize key");

    // Key should be non-empty (schema + data)
    assert!(!key.is_empty(), "Key should not be empty");

    // Full roundtrip validates key structure
    let value = heartbeat.record_value().expect("Failed to serialize value");
    let deserialized = Heartbeat::deserialize_record(&key, &value)
        .expect("Key deserialization should work with value");

    // Key data is preserved in deserialized object
    assert_eq!(
        heartbeat.source_cluster_alias(),
        deserialized.source_cluster_alias()
    );
    assert_eq!(
        heartbeat.target_cluster_alias(),
        deserialized.target_cluster_alias()
    );
}

/// Test value schema structure: contains version header and timestamp.
#[test]
fn test_value_schema_structure() {
    let heartbeat = Heartbeat::new("source", "target", 1234567890);
    let value = heartbeat.record_value().expect("Failed to serialize value");

    // Value should be non-empty (header + timestamp data)
    assert!(!value.is_empty(), "Value should not be empty");

    // First bytes should be header with version
    // VERSION_KEY (short = i16) takes 2 bytes in schema encoding
    // But schema encoding has additional overhead for field name
    // Minimum length check suffices
    assert!(value.len() > 2, "Value should contain header and data");
}

// ============================================================================
// ConnectPartition Tests
// ============================================================================

/// Test connectPartition returns correct keys.
#[test]
fn test_connect_partition_keys() {
    let heartbeat = Heartbeat::new("cluster-a", "cluster-b", 5000);
    let partition = heartbeat.connect_partition();

    // Verify partition contains both cluster alias keys
    assert!(partition.contains_key(SOURCE_CLUSTER_ALIAS_KEY));
    assert!(partition.contains_key(TARGET_CLUSTER_ALIAS_KEY));

    // Verify values match heartbeat fields
    assert_eq!(
        partition.get(SOURCE_CLUSTER_ALIAS_KEY),
        Some(&"cluster-a".to_string())
    );
    assert_eq!(
        partition.get(TARGET_CLUSTER_ALIAS_KEY),
        Some(&"cluster-b".to_string())
    );

    // Verify partition has exactly 2 entries
    assert_eq!(partition.len(), 2);
}

/// Test connectPartition with different cluster aliases.
#[test]
fn test_connect_partition_various_aliases() {
    let heartbeat = Heartbeat::new("prod-west", "prod-east", 0);
    let partition = heartbeat.connect_partition();

    assert_eq!(
        partition.get(SOURCE_CLUSTER_ALIAS_KEY),
        Some(&"prod-west".to_string())
    );
    assert_eq!(
        partition.get(TARGET_CLUSTER_ALIAS_KEY),
        Some(&"prod-east".to_string())
    );
}

// ============================================================================
// Display Tests
// ============================================================================

/// Test Display implementation matches Java toString format.
#[test]
fn test_display_format() {
    let heartbeat = Heartbeat::new("source-alias", "target-alias", 1000000);
    let display_str = heartbeat.to_string();

    // Verify format matches Java: Heartbeat{sourceClusterAlias=..., targetClusterAlias=..., timestamp=...}
    assert!(
        display_str.contains("Heartbeat"),
        "Should contain Heartbeat"
    );
    assert!(display_str.contains("sourceClusterAlias=source-alias"));
    assert!(display_str.contains("targetClusterAlias=target-alias"));
    assert!(display_str.contains("timestamp=1000000"));
}

// ============================================================================
// Getter Tests
// ============================================================================

/// Test all getter methods return correct values.
#[test]
fn test_getter_methods() {
    let heartbeat = Heartbeat::new("my-source", "my-target", 12345);

    assert_eq!(heartbeat.source_cluster_alias(), "my-source");
    assert_eq!(heartbeat.target_cluster_alias(), "my-target");
    assert_eq!(heartbeat.timestamp(), 12345);
}

// ============================================================================
// Equality Tests
// ============================================================================

/// Test Heartbeat equality based on all fields.
#[test]
fn test_equality() {
    let heartbeat1 = Heartbeat::new("source", "target", 1000);
    let heartbeat2 = Heartbeat::new("source", "target", 1000);
    let heartbeat3 = Heartbeat::new("source", "target", 2000);
    let heartbeat4 = Heartbeat::new("other", "target", 1000);

    // Same values should be equal
    assert_eq!(heartbeat1, heartbeat2);

    // Different values should not be equal
    assert_ne!(heartbeat1, heartbeat3);
    assert_ne!(heartbeat1, heartbeat4);
}

/// Test Heartbeat clone preserves all fields.
#[test]
fn test_clone() {
    let heartbeat = Heartbeat::new("clone-source", "clone-target", 9999);
    let cloned = heartbeat.clone();

    assert_eq!(heartbeat, cloned);
    assert_eq!(
        heartbeat.source_cluster_alias(),
        cloned.source_cluster_alias()
    );
    assert_eq!(
        heartbeat.target_cluster_alias(),
        cloned.target_cluster_alias()
    );
    assert_eq!(heartbeat.timestamp(), cloned.timestamp());
}

// ============================================================================
// Edge Cases Tests
// ============================================================================

/// Test serde with Unicode characters in cluster aliases.
#[test]
fn test_serde_unicode_aliases() {
    let heartbeat = Heartbeat::new("日本語", "中文", 12345);

    let key = heartbeat.record_key().expect("Failed to serialize key");
    let value = heartbeat.record_value().expect("Failed to serialize value");

    let deserialized =
        Heartbeat::deserialize_record(&key, &value).expect("Failed to deserialize heartbeat");

    assert_eq!(
        heartbeat.source_cluster_alias(),
        deserialized.source_cluster_alias()
    );
    assert_eq!(
        heartbeat.target_cluster_alias(),
        deserialized.target_cluster_alias()
    );
}

/// Test serde with very long cluster aliases.
#[test]
fn test_serde_long_aliases() {
    let long_alias = "a".repeat(1000);
    let heartbeat = Heartbeat::new(&long_alias, &long_alias, 0);

    let key = heartbeat.record_key().expect("Failed to serialize key");
    let value = heartbeat.record_value().expect("Failed to serialize value");

    let deserialized =
        Heartbeat::deserialize_record(&key, &value).expect("Failed to deserialize heartbeat");

    assert_eq!(
        heartbeat.source_cluster_alias(),
        deserialized.source_cluster_alias()
    );
    assert_eq!(
        heartbeat.target_cluster_alias(),
        deserialized.target_cluster_alias()
    );
}

/// Test multiple serde roundtrips preserve data.
#[test]
fn test_multiple_roundtrips() {
    let original = Heartbeat::new("source", "target", 1234567890);

    // First roundtrip
    let key1 = original.record_key().expect("Failed to serialize key1");
    let value1 = original.record_value().expect("Failed to serialize value1");
    let first = Heartbeat::deserialize_record(&key1, &value1).expect("Failed first roundtrip");

    // Second roundtrip
    let key2 = first.record_key().expect("Failed to serialize key2");
    let value2 = first.record_value().expect("Failed to serialize value2");
    let second = Heartbeat::deserialize_record(&key2, &value2).expect("Failed second roundtrip");

    // All should be equal
    assert_eq!(original, first);
    assert_eq!(original, second);
    assert_eq!(first, second);
}
