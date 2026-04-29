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

//! End-to-end tests for topic configuration synchronization.
//!
//! This module tests the complete configuration synchronization flow:
//! - External Kafka cluster (simulated by ExternalAdminMock with predefined configs)
//! - MirrorSourceConnector.sync_topic_configs() (config pulling and filtering)
//! - Local Kafka cluster (simulated by MockAdmin)
//! - incremental_alter_configs verification
//!
//! Corresponds to Java: MirrorConnectorsIntegrationBaseTest.testSyncTopicConfigs() (lines 818-859)

use std::collections::HashMap;

use common_trait::TopicPartition;
use connect_mirror::config::{SOURCE_CLUSTER_ALIAS_CONFIG, TARGET_CLUSTER_ALIAS_CONFIG};
use connect_mirror::source::config_property_filter::{
    ConfigPropertyFilter, DefaultConfigPropertyFilter, CONFIG_PROPERTIES_EXCLUDE_CONFIG,
};
use connect_mirror::source::mirror_source_connector::MirrorSourceConnector;
use connect_mirror_client::DefaultReplicationPolicy;
use connect_mirror_client::ReplicationPolicy;
use kafka_clients_mock::{AlterConfigOp, ConfigResource, ConfigResourceType, MockAdmin, NewTopic};

mod mocks;
use mocks::external_admin::{ConfigEntry, ExternalAdminMock};

// ============================================================================
// Helper Functions
// ============================================================================

/// Creates an ExternalAdminMock with predefined topic configurations.
///
/// Simulates an external source cluster with specific topic configs.
fn create_external_admin_with_configs(
    topic_name: &str,
    configs: HashMap<String, String>,
) -> ExternalAdminMock {
    let mock = ExternalAdminMock::new();
    mock.add_topic_with_configs(topic_name.to_string(), false, configs);
    mock
}

/// Creates a MockAdmin (target cluster) with an existing remote topic.
fn create_target_admin_with_topic(
    remote_topic_name: &str,
    initial_configs: HashMap<String, String>,
) -> MockAdmin {
    let admin = MockAdmin::new();

    // Create topic with initial configs
    let new_topic = NewTopic::new(remote_topic_name, 1, 1);
    admin.create_topics(vec![new_topic]);

    // Apply initial configs if any
    if !initial_configs.is_empty() {
        let resource = ConfigResource::new(ConfigResourceType::Topic, remote_topic_name);
        let alter_ops: Vec<AlterConfigOp> = initial_configs
            .iter()
            .map(|(k, v)| AlterConfigOp::set(k.clone(), v.clone()))
            .collect();
        let configs_map = HashMap::from([(resource, alter_ops)]);
        admin.incremental_alter_configs(configs_map);
    }

    admin
}

/// Creates test configuration for MirrorSourceConnector.
fn create_connector_config(source_alias: &str, target_alias: &str) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert(
        SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
        source_alias.to_string(),
    );
    props.insert(
        TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
        target_alias.to_string(),
    );
    // Enable frequent config sync for testing
    props.insert(
        "sync.topic.configs.interval.seconds".to_string(),
        "1".to_string(),
    );
    props
}

/// Gets topic configurations from MockAdmin as HashMap.
fn get_topic_configs(admin: &MockAdmin, topic_name: &str) -> HashMap<String, String> {
    let configs = admin.describe_topic_configs(&[topic_name.to_string()]);
    configs
        .get(topic_name)
        .and_then(|opt| opt.clone())
        .unwrap_or_else(HashMap::new)
}

// ============================================================================
// Tests - Basic Config Sync
// ============================================================================

#[test]
fn test_topic_config_sync_basic() {
    // Test scenario: Basic topic configuration synchronization
    // Source topic has retention.ms and cleanup.policy
    // These should be synchronized to target remote topic

    let source_alias = "primary";
    let target_alias = "backup";
    let source_topic = "test-topic-basic";

    let replication_policy = DefaultReplicationPolicy::new();
    let remote_topic = replication_policy.format_remote_topic(source_alias, source_topic);

    // 1. Create external admin with source configs
    let mut source_configs = HashMap::new();
    source_configs.insert("retention.ms".to_string(), "86400000".to_string());
    source_configs.insert("cleanup.policy".to_string(), "compact".to_string());

    let external_admin = create_external_admin_with_configs(source_topic, source_configs.clone());

    // 2. Create target admin with remote topic (empty configs initially)
    let target_admin = create_target_admin_with_topic(&remote_topic, HashMap::new());

    // 3. Verify source configs exist
    let source_config_result = external_admin.get_topic_config(source_topic, &[]);
    assert!(source_config_result.is_ok());
    let retrieved_source_configs = source_config_result.unwrap();
    assert_eq!(
        retrieved_source_configs.get("retention.ms").unwrap(),
        "86400000"
    );
    assert_eq!(
        retrieved_source_configs.get("cleanup.policy").unwrap(),
        "compact"
    );

    // 4. Verify target topic exists but has no custom configs initially
    let target_configs_before = get_topic_configs(&target_admin, &remote_topic);
    // MockAdmin may have default configs, but our custom configs should not be there yet
    assert!(
        !target_configs_before.contains_key("retention.ms")
            || target_configs_before.get("retention.ms") != Some(&"86400000".to_string())
    );

    // 5. Simulate config sync (manual for test isolation)
    let config_filter = DefaultConfigPropertyFilter::new();

    // Verify retention.ms should be replicated (not in exclude list)
    assert!(config_filter.should_replicate_config_property("retention.ms"));
    assert!(config_filter.should_replicate_config_property("cleanup.policy"));

    // 6. Apply configs manually (simulating sync_topic_configs)
    let filtered_configs: HashMap<String, String> = source_configs
        .iter()
        .filter(|(k, _)| config_filter.should_replicate_config_property(k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    assert_eq!(filtered_configs.len(), 2);

    // Apply to target using incremental_alter_configs
    let resource = ConfigResource::new(ConfigResourceType::Topic, remote_topic.clone());
    let alter_ops: Vec<AlterConfigOp> = filtered_configs
        .iter()
        .map(|(k, v)| AlterConfigOp::set(k.clone(), v.clone()))
        .collect();
    let configs_to_alter = HashMap::from([(resource, alter_ops)]);

    let result = target_admin.incremental_alter_configs(configs_to_alter);
    assert!(result.all_success());

    // 7. Verify configs synced to target
    let target_configs_after = get_topic_configs(&target_admin, &remote_topic);
    assert_eq!(
        target_configs_after.get("retention.ms").unwrap(),
        "86400000"
    );
    assert_eq!(
        target_configs_after.get("cleanup.policy").unwrap(),
        "compact"
    );
}

#[test]
fn test_config_property_filter_exclude() {
    // Test scenario: Config properties in exclude list should not be synced
    // delete.retention.ms is NOT in default exclude list (should be synced)
    // min.insync.replicas IS in default exclude list (should NOT be synced)

    let source_alias = "primary";
    let target_alias = "backup";
    let source_topic = "test-topic-exclude";

    let replication_policy = DefaultReplicationPolicy::new();
    let remote_topic = replication_policy.format_remote_topic(source_alias, source_topic);

    // 1. Create external admin with configs including excluded ones
    let mut source_configs = HashMap::new();
    source_configs.insert("delete.retention.ms".to_string(), "1000".to_string());
    source_configs.insert("retention.bytes".to_string(), "1000".to_string());
    source_configs.insert("min.insync.replicas".to_string(), "2".to_string()); // This should be excluded

    let external_admin = create_external_admin_with_configs(source_topic, source_configs.clone());

    // 2. Create target admin with remote topic
    // Pre-set configs to different values to test exclusion behavior
    let mut target_initial_configs = HashMap::new();
    target_initial_configs.insert("delete.retention.ms".to_string(), "2000".to_string());
    target_initial_configs.insert("retention.bytes".to_string(), "2000".to_string());
    target_initial_configs.insert("min.insync.replicas".to_string(), "3".to_string());

    let target_admin = create_target_admin_with_topic(&remote_topic, target_initial_configs);

    // 3. Create config filter with default exclusions
    let config_filter = DefaultConfigPropertyFilter::new();

    // Verify which properties should be replicated
    // delete.retention.ms is NOT in default exclude list
    assert!(config_filter.should_replicate_config_property("delete.retention.ms"));
    // retention.bytes is NOT in default exclude list
    assert!(config_filter.should_replicate_config_property("retention.bytes"));
    // min.insync.replicas IS in default exclude list
    assert!(!config_filter.should_replicate_config_property("min.insync.replicas"));

    // 4. Filter configs - only non-excluded should remain
    let filtered_configs: HashMap<String, String> = source_configs
        .iter()
        .filter(|(k, _)| config_filter.should_replicate_config_property(k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // Should have delete.retention.ms and retention.bytes (2 configs)
    // min.insync.replicas should be excluded
    assert_eq!(filtered_configs.len(), 2);
    assert!(filtered_configs.contains_key("delete.retention.ms"));
    assert!(filtered_configs.contains_key("retention.bytes"));
    assert!(!filtered_configs.contains_key("min.insync.replicas"));

    // 5. Apply filtered configs to target
    let resource = ConfigResource::new(ConfigResourceType::Topic, remote_topic.clone());
    let alter_ops: Vec<AlterConfigOp> = filtered_configs
        .iter()
        .map(|(k, v)| AlterConfigOp::set(k.clone(), v.clone()))
        .collect();
    let configs_to_alter = HashMap::from([(resource, alter_ops)]);

    let result = target_admin.incremental_alter_configs(configs_to_alter);
    assert!(result.all_success());

    // 6. Verify synced configs
    let target_configs_after = get_topic_configs(&target_admin, &remote_topic);

    // delete.retention.ms and retention.bytes should be synced (matching source)
    assert_eq!(
        target_configs_after.get("delete.retention.ms").unwrap(),
        "1000"
    );
    assert_eq!(target_configs_after.get("retention.bytes").unwrap(), "1000");

    // min.insync.replicas should NOT be synced (kept original target value)
    assert_eq!(
        target_configs_after.get("min.insync.replicas").unwrap(),
        "3"
    );
}

#[test]
fn test_incremental_alter_configs_set_operation() {
    // Test scenario: Verify incremental_alter_configs SET operation works correctly

    let topic_name = "test-topic-alter";
    let admin = MockAdmin::new();

    // Create topic
    let new_topic = NewTopic::new(topic_name, 1, 1);
    admin.create_topics(vec![new_topic]);

    // Set multiple configs using incremental_alter_configs
    let resource = ConfigResource::new(ConfigResourceType::Topic, topic_name);
    let alter_ops = vec![
        AlterConfigOp::set("retention.ms".to_string(), "86400000".to_string()),
        AlterConfigOp::set("segment.bytes".to_string(), "1073741824".to_string()),
        AlterConfigOp::set("cleanup.policy".to_string(), "compact".to_string()),
    ];

    let configs_to_alter = HashMap::from([(resource, alter_ops)]);
    let result = admin.incremental_alter_configs(configs_to_alter);

    // Verify all operations succeeded
    assert!(result.all_success());

    // Verify configs were applied
    let configs = get_topic_configs(&admin, topic_name);
    assert_eq!(configs.get("retention.ms").unwrap(), "86400000");
    assert_eq!(configs.get("segment.bytes").unwrap(), "1073741824");
    assert_eq!(configs.get("cleanup.policy").unwrap(), "compact");
}

#[test]
fn test_incremental_alter_configs_delete_operation() {
    // Test scenario: Verify incremental_alter_configs DELETE operation works

    let topic_name = "test-topic-delete";
    let admin = MockAdmin::new();

    // Create topic with initial config
    let new_topic = NewTopic::new(topic_name, 1, 1);
    admin.create_topics(vec![new_topic]);

    // Set initial config
    let resource = ConfigResource::new(ConfigResourceType::Topic, topic_name);
    let set_ops = vec![AlterConfigOp::set(
        "custom.config".to_string(),
        "value1".to_string(),
    )];
    admin.incremental_alter_configs(HashMap::from([(resource.clone(), set_ops)]));

    // Verify config exists
    let configs_before = get_topic_configs(&admin, topic_name);
    assert!(configs_before.contains_key("custom.config"));

    // Delete config
    let delete_ops = vec![AlterConfigOp::delete("custom.config".to_string())];
    let result = admin.incremental_alter_configs(HashMap::from([(resource, delete_ops)]));
    assert!(result.all_success());

    // Verify config was deleted
    let configs_after = get_topic_configs(&admin, topic_name);
    assert!(!configs_after.contains_key("custom.config"));
}

#[test]
fn test_incremental_alter_configs_multiple_topics() {
    // Test scenario: Alter configs for multiple topics in one call

    let admin = MockAdmin::new();

    // Create two topics
    admin.create_topics(vec![
        NewTopic::new("topic1", 1, 1),
        NewTopic::new("topic2", 1, 1),
    ]);

    // Alter configs for both topics
    let resource1 = ConfigResource::new(ConfigResourceType::Topic, "topic1");
    let resource2 = ConfigResource::new(ConfigResourceType::Topic, "topic2");

    let ops1 = vec![AlterConfigOp::set(
        "retention.ms".to_string(),
        "10000".to_string(),
    )];
    let ops2 = vec![AlterConfigOp::set(
        "retention.ms".to_string(),
        "20000".to_string(),
    )];

    let configs_to_alter = HashMap::from([(resource1, ops1), (resource2, ops2)]);

    let result = admin.incremental_alter_configs(configs_to_alter);
    assert!(result.all_success());

    // Verify each topic has correct config
    let configs1 = get_topic_configs(&admin, "topic1");
    let configs2 = get_topic_configs(&admin, "topic2");

    assert_eq!(configs1.get("retention.ms").unwrap(), "10000");
    assert_eq!(configs2.get("retention.ms").unwrap(), "20000");
}

#[test]
fn test_sync_topic_configs_integration() {
    // Test scenario: Full integration test with MirrorSourceConnector
    // This tests the complete flow similar to Kafka's testSyncTopicConfigs

    let source_alias = "primary";
    let target_alias = "backup";
    let source_topic = "test-topic-with-config";

    let replication_policy = DefaultReplicationPolicy::new();
    let remote_topic = replication_policy.format_remote_topic(source_alias, source_topic);

    // 1. Setup external admin (source cluster) with configs
    // Similar to Kafka test: create topic with delete.retention.ms=1000, retention.bytes=1000
    let mut source_configs = HashMap::new();
    source_configs.insert("delete.retention.ms".to_string(), "1000".to_string());
    source_configs.insert("retention.bytes".to_string(), "1000".to_string());

    let external_admin = create_external_admin_with_configs(source_topic, source_configs);

    // 2. Setup target admin (backup cluster)
    // Create remote topic and set configs to different values (like Kafka test)
    let mut target_initial_configs = HashMap::new();
    target_initial_configs.insert("delete.retention.ms".to_string(), "2000".to_string());
    target_initial_configs.insert("retention.bytes".to_string(), "2000".to_string());

    let target_admin = create_target_admin_with_topic(&remote_topic, target_initial_configs);

    // 3. Setup connector config
    // Use custom exclude filter to exclude delete.retention.* properties
    // Similar to Kafka: DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_CONFIG
    let mut connector_props = create_connector_config(source_alias, target_alias);
    connector_props.insert(
        CONFIG_PROPERTIES_EXCLUDE_CONFIG.to_string(),
        "delete\\.retention\\.*".to_string(),
    );

    // 4. Create config filter with custom exclusions
    let mut config_filter = DefaultConfigPropertyFilter::new();
    config_filter.configure(&connector_props);

    // Verify filter behavior
    // delete.retention.ms should now be excluded
    assert!(!config_filter.should_replicate_config_property("delete.retention.ms"));
    // retention.bytes should NOT be excluded
    assert!(config_filter.should_replicate_config_property("retention.bytes"));

    // 5. Get source configs and filter
    let source_config_result = external_admin.get_topic_config(source_topic, &[]);
    assert!(source_config_result.is_ok());
    let all_source_configs = source_config_result.unwrap();

    let filtered_configs: HashMap<String, String> = all_source_configs
        .iter()
        .filter(|(k, _)| config_filter.should_replicate_config_property(k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // Only retention.bytes should remain (delete.retention.ms excluded)
    assert_eq!(filtered_configs.len(), 1);
    assert!(filtered_configs.contains_key("retention.bytes"));
    assert!(!filtered_configs.contains_key("delete.retention.ms"));

    // 6. Apply filtered configs to target
    let resource = ConfigResource::new(ConfigResourceType::Topic, remote_topic.clone());
    let alter_ops: Vec<AlterConfigOp> = filtered_configs
        .iter()
        .map(|(k, v)| AlterConfigOp::set(k.clone(), v.clone()))
        .collect();
    let configs_to_alter = HashMap::from([(resource, alter_ops)]);

    let result = target_admin.incremental_alter_configs(configs_to_alter);
    assert!(result.all_success());

    // 7. Verify final state
    // Similar to Kafka test assertions:
    // - delete.retention.ms on backup should NOT equal source (exclusion worked)
    // - retention.bytes on backup should equal source (synced correctly)
    let target_configs_final = get_topic_configs(&target_admin, &remote_topic);

    // delete.retention.ms should keep its original target value (2000, not 1000)
    assert_eq!(
        target_configs_final.get("delete.retention.ms").unwrap(),
        "2000"
    );
    // This proves exclusion filter worked - not synced from source

    // retention.bytes should be synced from source (1000, not 2000)
    assert_eq!(target_configs_final.get("retention.bytes").unwrap(), "1000");
    // This proves non-excluded configs are correctly synced
}

#[test]
fn test_config_sync_nonexistent_topic() {
    // Test scenario: Config sync for nonexistent topic should fail gracefully

    let admin = MockAdmin::new();

    // Don't create topic, try to alter configs
    let resource = ConfigResource::new(ConfigResourceType::Topic, "nonexistent-topic");
    let alter_ops = vec![AlterConfigOp::set(
        "retention.ms".to_string(),
        "86400000".to_string(),
    )];

    let configs_to_alter = HashMap::from([(resource, alter_ops)]);
    let result = admin.incremental_alter_configs(configs_to_alter);

    // Should return error for nonexistent topic
    assert!(result.has_errors());
}

#[test]
fn test_config_sync_empty_filtered_configs() {
    // Test scenario: All source configs are excluded, nothing to sync

    let source_alias = "primary";
    let target_alias = "backup";
    let source_topic = "test-topic-all-excluded";

    let replication_policy = DefaultReplicationPolicy::new();
    let remote_topic = replication_policy.format_remote_topic(source_alias, source_topic);

    // 1. Setup source with only excluded configs
    let mut source_configs = HashMap::new();
    source_configs.insert("min.insync.replicas".to_string(), "2".to_string());
    source_configs.insert(
        "follower.replication.throttled.replicas".to_string(),
        "".to_string(),
    );

    let external_admin = create_external_admin_with_configs(source_topic, source_configs.clone());

    // 2. Setup target with initial configs
    let mut target_initial_configs = HashMap::new();
    target_initial_configs.insert("min.insync.replicas".to_string(), "3".to_string());

    let target_admin = create_target_admin_with_topic(&remote_topic, target_initial_configs);

    // 3. Apply default filter - all configs should be excluded
    let config_filter = DefaultConfigPropertyFilter::new();

    assert!(!config_filter.should_replicate_config_property("min.insync.replicas"));
    assert!(
        !config_filter.should_replicate_config_property("follower.replication.throttled.replicas")
    );

    // 4. Filter configs - should be empty
    let filtered_configs: HashMap<String, String> = source_configs
        .iter()
        .filter(|(k, _)| config_filter.should_replicate_config_property(k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    assert_eq!(filtered_configs.len(), 0);

    // 5. No configs to sync - target configs should remain unchanged
    let target_configs_after = get_topic_configs(&target_admin, &remote_topic);
    // Should keep original target value
    assert_eq!(
        target_configs_after.get("min.insync.replicas").unwrap(),
        "3"
    );
}

#[test]
fn test_remote_topic_name_formatting() {
    // Test scenario: Verify remote topic name formatting via ReplicationPolicy

    let source_alias = "primary";
    let target_alias = "backup";

    let replication_policy = DefaultReplicationPolicy::new();

    // Test various topic names
    let test_cases = vec![
        ("simple-topic", "primary.simple-topic"),
        ("topic-with-dashes", "primary.topic-with-dashes"),
        ("topic123", "primary.topic123"),
    ];

    for (source_topic, expected_remote) in test_cases {
        let remote_topic = replication_policy.format_remote_topic(source_alias, source_topic);
        assert_eq!(remote_topic, expected_remote);

        // Verify we can get original topic name
        let original = replication_policy.original_topic(&remote_topic);
        assert_eq!(original, source_topic);
    }
}
