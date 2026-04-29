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

//! Tests for Heartbeat module
//! Corresponds to Java: org.apache.kafka.connect.mirror.Heartbeat

use connect_mirror_client::{
    Heartbeat, SOURCE_CLUSTER_ALIAS_KEY, TARGET_CLUSTER_ALIAS_KEY, TIMESTAMP_KEY,
};

// ============================================================================
// ConnectPartition Tests
// ============================================================================

#[test]
fn test_connect_partition() {
    let heartbeat = Heartbeat::new("source-a", "target-a", 123);
    let partition = heartbeat.connect_partition();

    assert_eq!(
        partition.get(SOURCE_CLUSTER_ALIAS_KEY),
        Some(&"source-a".to_string())
    );
    assert_eq!(
        partition.get(TARGET_CLUSTER_ALIAS_KEY),
        Some(&"target-a".to_string())
    );
}

#[test]
fn test_connect_partition_keys_only() {
    let heartbeat = Heartbeat::new("prod-source", "prod-target", 0);
    let partition = heartbeat.connect_partition();

    // Partition should contain exactly 2 keys
    assert_eq!(partition.len(), 2);
    assert!(partition.contains_key(SOURCE_CLUSTER_ALIAS_KEY));
    assert!(partition.contains_key(TARGET_CLUSTER_ALIAS_KEY));
}

// ============================================================================
// Serde Tests (Corresponds to Java HeartbeatTest.testSerde)
// ============================================================================

/// Test serde roundtrip - corresponds to Java HeartbeatTest.testSerde()
#[test]
fn test_serde_roundtrip() {
    // Java test uses: ("source-1", "target-2", 1234567890L)
    let heartbeat = Heartbeat::new("source-1", "target-2", 1234567890);

    let key = heartbeat.record_key().expect("Failed to serialize key");
    let value = heartbeat.record_value().expect("Failed to serialize value");

    let deserialized = Heartbeat::deserialize_record(&key, &value).expect("Failed to deserialize");

    // Java assertions with failure messages
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

#[test]
fn test_serde_with_empty_aliases() {
    let heartbeat = Heartbeat::new("", "", 0);

    let key = heartbeat.record_key().expect("Failed to serialize key");
    let value = heartbeat.record_value().expect("Failed to serialize value");

    let deserialized = Heartbeat::deserialize_record(&key, &value).expect("Failed");

    assert_eq!(heartbeat, deserialized);
}

#[test]
fn test_serde_with_negative_timestamp() {
    let heartbeat = Heartbeat::new("source", "target", -1);

    let key = heartbeat.record_key().expect("Failed to serialize key");
    let value = heartbeat.record_value().expect("Failed to serialize value");

    let deserialized = Heartbeat::deserialize_record(&key, &value).expect("Failed");

    assert_eq!(heartbeat.timestamp(), deserialized.timestamp());
}

// ============================================================================
// Getter Tests
// ============================================================================

#[test]
fn test_getter_methods() {
    let heartbeat = Heartbeat::new("source-cluster", "target-cluster", 10000);

    assert_eq!(heartbeat.source_cluster_alias(), "source-cluster");
    assert_eq!(heartbeat.target_cluster_alias(), "target-cluster");
    assert_eq!(heartbeat.timestamp(), 10000);
}

// ============================================================================
// Equality and Clone Tests
// ============================================================================

#[test]
fn test_equality() {
    let h1 = Heartbeat::new("a", "b", 100);
    let h2 = Heartbeat::new("a", "b", 100);
    let h3 = Heartbeat::new("a", "b", 200);

    assert_eq!(h1, h2);
    assert_ne!(h1, h3);
}

#[test]
fn test_clone() {
    let heartbeat = Heartbeat::new("clone-source", "clone-target", 5000);
    let cloned = heartbeat.clone();

    assert_eq!(heartbeat, cloned);
}

// ============================================================================
// Display Tests
// ============================================================================

#[test]
fn test_display_format() {
    let heartbeat = Heartbeat::new("source", "target", 1000000);
    let display = heartbeat.to_string();

    assert!(display.contains("Heartbeat"));
    assert!(display.contains("sourceClusterAlias=source"));
    assert!(display.contains("targetClusterAlias=target"));
    assert!(display.contains("timestamp=1000000"));
}
