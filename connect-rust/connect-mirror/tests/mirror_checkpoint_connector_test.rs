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

//! Tests for MirrorCheckpointConnector.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorCheckpointConnectorTest
//!
//! Test coverage includes:
//! - start/stop lifecycle
//! - emit.checkpoints.enabled/disabled configuration
//! - task_configs distribution and consumer group assignment
//! - filtering behavior (GroupFilter + TopicFilter combination)
//! - validation of required configuration fields
//! - SourceConnector trait methods (exactly_once_support, alter_offsets)
//! - initialize_with_task_configs state restoration

use std::collections::HashMap;
use std::sync::Arc;

use connect_api::components::Versioned;
use connect_api::connector::{Connector, ConnectorContext};
use connect_api::errors::ConnectError;
use connect_api::source::{ConnectorTransactionBoundaries, ExactlyOnceSupport, SourceConnector};
use connect_mirror::checkpoint::{
    MirrorCheckpointConfig, MirrorCheckpointConnector, EMIT_CHECKPOINTS_ENABLED_CONFIG,
    EMIT_CHECKPOINTS_INTERVAL_SECONDS_CONFIG, TASK_ASSIGNED_GROUPS_CONFIG, TASK_INDEX_CONFIG,
};
use connect_mirror::config::{SOURCE_CLUSTER_ALIAS_CONFIG, TARGET_CLUSTER_ALIAS_CONFIG};
use kafka_clients_mock::{MockAdmin, NewTopic};

// ============================================================================
// Mock ConnectorContext for testing
// ============================================================================

/// Mock implementation of ConnectorContext for testing.
struct MockConnectorContext {
    request_task_reconfiguration_called: bool,
}

impl MockConnectorContext {
    fn new() -> Self {
        MockConnectorContext {
            request_task_reconfiguration_called: false,
        }
    }
}

impl ConnectorContext for MockConnectorContext {
    fn request_task_reconfiguration(&mut self) {
        self.request_task_reconfiguration_called = true;
    }

    fn raise_error(&mut self, _error: ConnectError) {
        // Mock implementation - does nothing
    }
}

// ============================================================================
// Test helper functions
// ============================================================================

/// Creates a basic test configuration for MirrorCheckpointConnector.
fn create_basic_config() -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert(
        SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
        "backup".to_string(),
    );
    props.insert(
        TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
        "primary".to_string(),
    );
    props.insert(
        "name".to_string(),
        "mirror-checkpoint-connector".to_string(),
    );
    props.insert("groups".to_string(), ".*".to_string());
    props.insert("topics".to_string(), ".*".to_string());
    props
}

/// Creates a configuration with emit.checkpoints.enabled=false.
fn create_config_with_checkpoints_disabled() -> HashMap<String, String> {
    let mut props = create_basic_config();
    props.insert(
        EMIT_CHECKPOINTS_ENABLED_CONFIG.to_string(),
        "false".to_string(),
    );
    props
}

/// Creates a configuration with emit.checkpoints.enabled=true.
fn create_config_with_checkpoints_enabled() -> HashMap<String, String> {
    let mut props = create_basic_config();
    props.insert(
        EMIT_CHECKPOINTS_ENABLED_CONFIG.to_string(),
        "true".to_string(),
    );
    props
}

/// Creates a configuration with zero emit.checkpoints.interval.seconds.
fn create_config_with_zero_interval() -> HashMap<String, String> {
    let mut props = create_basic_config();
    props.insert(
        EMIT_CHECKPOINTS_INTERVAL_SECONDS_CONFIG.to_string(),
        "0".to_string(),
    );
    props
}

/// Creates a MockAdmin with test topics.
fn create_admin_with_topics(topics: &[&str]) -> Arc<MockAdmin> {
    let admin = Arc::new(MockAdmin::new());
    for topic in topics {
        admin.create_topics(vec![NewTopic::new(topic.to_string(), 3, 1)]);
    }
    admin
}

// ============================================================================
// Lifecycle Tests (start/stop)
// ============================================================================

#[test]
fn test_new_connector() {
    let connector = MirrorCheckpointConnector::new();

    // New connector should not be started
    assert!(!connector.is_started());

    // New connector should not have initial groups loaded
    assert!(!connector.is_initial_groups_loaded());

    // New connector should have empty known consumer groups
    assert!(connector.known_consumer_groups().is_empty());
}

#[test]
fn test_version() {
    // Version is accessed through the Versioned trait
    assert_eq!(MirrorCheckpointConnector::version(), "1.0.0");
}

#[test]
fn test_task_class() {
    let connector = MirrorCheckpointConnector::new();

    // Task class should be MirrorCheckpointTask
    assert_eq!(connector.task_class(), "MirrorCheckpointTask");
}

#[test]
fn test_start_stop_cycle() {
    let mut connector = MirrorCheckpointConnector::new();
    let props = create_basic_config();

    // Initialize connector
    connector.initialize(Box::new(MockConnectorContext::new()));

    // Start connector
    connector.start(props.clone());

    // Connector should be started
    assert!(connector.is_started());

    // Configuration should be set
    assert!(connector.config_ref().is_some());

    // Filters should be set
    assert!(connector.group_filter().is_some());
    assert!(connector.topic_filter().is_some());

    // Initial groups should be loaded (even if empty in mock)
    assert!(connector.is_initial_groups_loaded());

    // Stop connector
    connector.stop();

    // Connector should be stopped
    assert!(!connector.is_started());

    // Known consumer groups should be cleared
    assert!(connector.known_consumer_groups().is_empty());

    // Filters should be cleared
    assert!(connector.group_filter().is_none());
    assert!(connector.topic_filter().is_none());
}

#[test]
fn test_with_admin_clients() {
    let source_admin = Arc::new(MockAdmin::new());
    let target_admin = Arc::new(MockAdmin::new());

    let connector =
        MirrorCheckpointConnector::with_admin_clients(source_admin.clone(), target_admin.clone());

    // Should have the provided admin clients
    assert!(Arc::ptr_eq(&connector.source_admin(), &source_admin));
    assert!(Arc::ptr_eq(&connector.target_admin(), &target_admin));
}

// ============================================================================
// Enabled/Disabled Configuration Tests
// Corresponds to Java: testMirrorCheckpointConnectorDisabled, testMirrorCheckpointConnectorEnabled
// ============================================================================

#[test]
fn test_mirror_checkpoint_connector_disabled() {
    // Corresponds to Java: testMirrorCheckpointConnectorDisabled
    // When emit.checkpoints.enabled=false, taskConfigs returns empty

    let mut connector = MirrorCheckpointConnector::new();
    let props = create_config_with_checkpoints_disabled();

    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(props);

    // Connector should be started but disabled
    assert!(connector.is_started());

    // Add a consumer group to test the disabled behavior
    connector.add_known_consumer_group("test-group".to_string());
    connector.set_initial_groups_loaded(true);

    // task_configs should return empty when disabled
    let configs = connector.task_configs(1).unwrap();
    assert!(configs.is_empty());
}

#[test]
fn test_mirror_checkpoint_connector_enabled() {
    // Corresponds to Java: testMirrorCheckpointConnectorEnabled
    // When emit.checkpoints.enabled=true, taskConfigs returns task configs

    let mut connector = MirrorCheckpointConnector::new();
    let props = create_config_with_checkpoints_enabled();

    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(props);

    // Connector should be started
    assert!(connector.is_started());

    // Should have initial groups loaded
    assert!(connector.is_initial_groups_loaded());

    // Add groups manually for test
    connector.add_known_consumer_group("consumer-group-1".to_string());
    connector.set_initial_groups_loaded(true);

    // task_configs should return at least one task
    let configs = connector.task_configs(1).unwrap();
    assert!(!configs.is_empty());
}

#[test]
fn test_replication_disabled() {
    // Corresponds to Java: testReplicationDisabled
    // Similar to testMirrorCheckpointConnectorDisabled

    let mut connector = MirrorCheckpointConnector::new();
    let props = create_config_with_checkpoints_disabled();

    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(props);

    connector.set_initial_groups_loaded(true);
    connector.add_known_consumer_group("group1".to_string());

    let configs = connector.task_configs(1).unwrap();
    assert!(configs.is_empty());
}

#[test]
fn test_replication_enabled() {
    // Corresponds to Java: testReplicationEnabled

    let mut connector = MirrorCheckpointConnector::new();
    let props = create_config_with_checkpoints_enabled();

    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(props);

    connector.set_initial_groups_loaded(true);
    connector.add_known_consumer_group("group1".to_string());

    // When enabled and groups present, should return task configs
    let configs = connector.task_configs(1).unwrap();
    assert!(!configs.is_empty());
    assert_eq!(configs.len(), 1);

    // Task config should contain assigned groups
    assert!(configs[0].contains_key(TASK_ASSIGNED_GROUPS_CONFIG));
}

#[test]
fn test_emit_checkpoints_zero_interval() {
    // When emit.checkpoints.interval.seconds=0, taskConfigs returns empty

    let mut connector = MirrorCheckpointConnector::new();
    let props = create_config_with_zero_interval();

    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(props);

    connector.set_initial_groups_loaded(true);
    connector.add_known_consumer_group("group1".to_string());

    // Zero interval means disabled
    let configs = connector.task_configs(1).unwrap();
    assert!(configs.is_empty());
}

// ============================================================================
// Task Configs Distribution Tests
// Corresponds to Java: testNoConsumerGroup, testConsumerGroupInitializeTimeout
// ============================================================================

#[test]
fn test_no_consumer_group() {
    // Corresponds to Java: testNoConsumerGroup
    // When no consumer groups, taskConfigs returns empty

    let mut connector = MirrorCheckpointConnector::new();
    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(create_basic_config());
    connector.set_initial_groups_loaded(true);

    // No groups set
    let configs = connector.task_configs(1).unwrap();
    assert!(configs.is_empty());
}

#[test]
fn test_consumer_group_initialize_timeout() {
    // Corresponds to Java: testConsumerGroupInitializeTimeout
    // When knownConsumerGroups is null (initial_groups_loaded=false), throws RetriableException

    let connector = MirrorCheckpointConnector::new();

    // Connector not started, initial_groups_loaded=false
    // task_configs should return a Retriable error
    let result = connector.task_configs(1);
    assert!(result.is_err());

    // The error should be retriable
    let err = result.unwrap_err();
    assert!(err.is_retriable());
}

#[test]
fn test_task_configs_not_started() {
    // Connector not started should return error

    let connector = MirrorCheckpointConnector::new();
    let result = connector.task_configs(1);
    assert!(result.is_err());
}

#[test]
fn test_task_configs_empty_groups() {
    // Started but empty groups should return empty configs

    let mut connector = MirrorCheckpointConnector::new();
    connector.set_started(true);
    connector.set_initial_groups_loaded(true);

    let configs = connector.task_configs(1).unwrap();
    assert!(configs.is_empty());
}

#[test]
fn test_task_configs_distribution_round_robin() {
    // Consumer groups should be distributed round-robin across tasks

    let mut connector = MirrorCheckpointConnector::new();
    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(create_basic_config());
    connector.set_initial_groups_loaded(true);

    // Add multiple consumer groups
    connector.add_known_consumer_group("group1".to_string());
    connector.add_known_consumer_group("group2".to_string());
    connector.add_known_consumer_group("group3".to_string());
    connector.add_known_consumer_group("group4".to_string());

    let configs = connector.task_configs(2).unwrap();
    assert_eq!(configs.len(), 2);

    // Each config should have task index and assigned groups
    for config in &configs {
        assert!(config.contains_key(TASK_INDEX_CONFIG));
        assert!(config.contains_key(TASK_ASSIGNED_GROUPS_CONFIG));
    }
}

#[test]
fn test_task_configs_respects_max_tasks() {
    // When max_tasks > number of groups, should not create empty tasks

    let mut connector = MirrorCheckpointConnector::new();
    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(create_basic_config());
    connector.set_initial_groups_loaded(true);

    // Add fewer groups than requested tasks
    connector.add_known_consumer_group("group1".to_string());

    let configs = connector.task_configs(10).unwrap();

    // Should only create 1 task (one group)
    assert_eq!(configs.len(), 1);
}

#[test]
fn test_task_configs_zero_max_tasks() {
    // max_tasks=0 should return empty

    let mut connector = MirrorCheckpointConnector::new();
    connector.set_started(true);
    connector.set_initial_groups_loaded(true);
    connector.add_known_consumer_group("group1".to_string());

    let configs = connector.task_configs(0).unwrap();
    assert!(configs.is_empty());
}

#[test]
fn test_task_configs_single_group() {
    // Single group should create single task

    let mut connector = MirrorCheckpointConnector::new();
    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(create_basic_config());
    connector.set_initial_groups_loaded(true);

    connector.add_known_consumer_group("single-group".to_string());

    let configs = connector.task_configs(3).unwrap();
    assert_eq!(configs.len(), 1);

    // Task config should contain the group
    let groups_str = configs[0].get(TASK_ASSIGNED_GROUPS_CONFIG).unwrap();
    assert_eq!(groups_str, "single-group");
}

#[test]
fn test_task_configs_source_target_alias_included() {
    // Task configs should include source and target cluster aliases

    let mut connector = MirrorCheckpointConnector::new();
    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(create_basic_config());
    connector.set_initial_groups_loaded(true);

    connector.add_known_consumer_group("group1".to_string());

    let configs = connector.task_configs(1).unwrap();
    assert!(!configs.is_empty());

    // Should include source.cluster.alias and target.cluster.alias
    assert!(configs[0].contains_key("source.cluster.alias"));
    assert!(configs[0].contains_key("target.cluster.alias"));
    assert_eq!(configs[0].get("source.cluster.alias").unwrap(), "backup");
    assert_eq!(configs[0].get("target.cluster.alias").unwrap(), "primary");
}

// ============================================================================
// Validation Tests
// ============================================================================

#[test]
fn test_validate_missing_source_alias() {
    let connector = MirrorCheckpointConnector::new();
    let mut props = HashMap::new();
    props.insert(
        TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
        "primary".to_string(),
    );

    let config = connector.validate(props);

    // Should have error for missing source.cluster.alias
    let has_source_error = config
        .config_values()
        .iter()
        .any(|cv| cv.name() == SOURCE_CLUSTER_ALIAS_CONFIG && !cv.error_messages().is_empty());
    assert!(has_source_error);
}

#[test]
fn test_validate_missing_target_alias() {
    let connector = MirrorCheckpointConnector::new();
    let mut props = HashMap::new();
    props.insert(
        SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
        "backup".to_string(),
    );

    let config = connector.validate(props);

    // Should have error for missing target.cluster.alias
    let has_target_error = config
        .config_values()
        .iter()
        .any(|cv| cv.name() == TARGET_CLUSTER_ALIAS_CONFIG && !cv.error_messages().is_empty());
    assert!(has_target_error);
}

#[test]
fn test_validate_valid_config() {
    let connector = MirrorCheckpointConnector::new();
    let props = create_basic_config();

    let config = connector.validate(props);

    // Should have no errors for valid config
    let has_errors = config
        .config_values()
        .iter()
        .any(|cv| !cv.error_messages().is_empty());
    assert!(!has_errors);
}

#[test]
fn test_validate_both_aliases_missing() {
    let connector = MirrorCheckpointConnector::new();
    let props = HashMap::new();

    let config = connector.validate(props);

    // Should have errors for both missing aliases
    let has_source_error = config
        .config_values()
        .iter()
        .any(|cv| cv.name() == SOURCE_CLUSTER_ALIAS_CONFIG && !cv.error_messages().is_empty());
    let has_target_error = config
        .config_values()
        .iter()
        .any(|cv| cv.name() == TARGET_CLUSTER_ALIAS_CONFIG && !cv.error_messages().is_empty());

    assert!(has_source_error);
    assert!(has_target_error);
}

// ============================================================================
// SourceConnector Trait Tests
// ============================================================================

#[test]
fn test_exactly_once_support_unsupported() {
    let connector = MirrorCheckpointConnector::new();

    // MirrorCheckpointConnector does not support exactly-once
    let support = connector.exactly_once_support(HashMap::new());
    assert_eq!(support, ExactlyOnceSupport::Unsupported);
}

#[test]
fn test_can_define_transaction_boundaries_coordinator_defined() {
    let connector = MirrorCheckpointConnector::new();

    // Transaction boundaries are coordinator-defined
    let boundaries = connector.can_define_transaction_boundaries(HashMap::new());
    assert_eq!(
        boundaries,
        ConnectorTransactionBoundaries::CoordinatorDefined
    );
}

#[test]
fn test_alter_offsets_returns_false() {
    // alter_offsets does not support external offset management
    // Corresponds to Java: MirrorCheckpointConnector.alterOffsets() validation tests
    // Note: Java tests validate input structure, but Rust implementation
    // simplifies to return false (not supported)

    let connector = MirrorCheckpointConnector::new();

    // Empty offsets map
    let result = connector.alter_offsets(HashMap::new(), HashMap::new());
    assert_eq!(result.unwrap(), false);
}

// ============================================================================
// Initialize with Task Configs Tests
// ============================================================================

#[test]
fn test_initialize_with_task_configs() {
    // Corresponds to Java: restore state from task configs

    let mut connector = MirrorCheckpointConnector::new();

    let task_configs = vec![
        HashMap::from([
            (
                TASK_ASSIGNED_GROUPS_CONFIG.to_string(),
                "group1,group2".to_string(),
            ),
            (TASK_INDEX_CONFIG.to_string(), "0".to_string()),
        ]),
        HashMap::from([
            (
                TASK_ASSIGNED_GROUPS_CONFIG.to_string(),
                "group3".to_string(),
            ),
            (TASK_INDEX_CONFIG.to_string(), "1".to_string()),
        ]),
    ];

    connector.initialize_with_task_configs(Box::new(MockConnectorContext::new()), task_configs);

    // Should have restored groups from task configs
    assert!(connector.is_initial_groups_loaded());
    assert!(connector.known_consumer_groups().contains("group1"));
    assert!(connector.known_consumer_groups().contains("group2"));
    assert!(connector.known_consumer_groups().contains("group3"));
}

#[test]
fn test_initialize_with_task_configs_empty() {
    let mut connector = MirrorCheckpointConnector::new();

    connector.initialize_with_task_configs(Box::new(MockConnectorContext::new()), vec![]);

    // Groups should be empty
    assert!(connector.known_consumer_groups().is_empty());

    // Should be marked as loaded (restoring empty state)
    assert!(connector.is_initial_groups_loaded());
}

#[test]
fn test_initialize_with_task_configs_whitespace_handling() {
    let mut connector = MirrorCheckpointConnector::new();

    // Groups with whitespace should be trimmed
    let task_configs = vec![HashMap::from([
        (
            TASK_ASSIGNED_GROUPS_CONFIG.to_string(),
            "group1, group2 ,group3".to_string(),
        ),
        (TASK_INDEX_CONFIG.to_string(), "0".to_string()),
    ])];

    connector.initialize_with_task_configs(Box::new(MockConnectorContext::new()), task_configs);

    // Whitespace should be trimmed
    assert!(connector.known_consumer_groups().contains("group1"));
    assert!(connector.known_consumer_groups().contains("group2"));
    assert!(connector.known_consumer_groups().contains("group3"));
}

// ============================================================================
// Partition Groups Helper Tests
// ============================================================================

#[test]
fn test_partition_groups_basic() {
    // Round-robin partitioning

    let items = vec!["a", "b", "c", "d", "e"];
    let partitions = MirrorCheckpointConnector::partition_groups(&items, 2);

    assert_eq!(partitions.len(), 2);
    // Round-robin: a,c,e -> partition 0; b,d -> partition 1
    assert_eq!(partitions[0], vec!["a", "c", "e"]);
    assert_eq!(partitions[1], vec!["b", "d"]);
}

#[test]
fn test_partition_groups_empty_items() {
    let items: Vec<String> = vec![];
    let partitions = MirrorCheckpointConnector::partition_groups(&items, 2);
    assert!(partitions.is_empty());
}

#[test]
fn test_partition_groups_zero_tasks() {
    let items = vec!["a", "b"];
    let partitions = MirrorCheckpointConnector::partition_groups(&items, 0);
    assert!(partitions.is_empty());
}

#[test]
fn test_partition_groups_negative_tasks() {
    let items = vec!["a", "b"];
    let partitions = MirrorCheckpointConnector::partition_groups(&items, -1);
    assert!(partitions.is_empty());
}

#[test]
fn test_partition_groups_single_task() {
    let items = vec!["a", "b", "c"];
    let partitions = MirrorCheckpointConnector::partition_groups(&items, 1);

    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0], vec!["a", "b", "c"]);
}

#[test]
fn test_partition_groups_more_tasks_than_items() {
    let items = vec!["a"];
    let partitions = MirrorCheckpointConnector::partition_groups(&items, 5);

    assert_eq!(partitions.len(), 5);
    // Only first partition has items
    assert_eq!(partitions[0], vec!["a"]);
    assert!(partitions[1].is_empty());
    assert!(partitions[2].is_empty());
}

// ============================================================================
// Connector State Tests
// ============================================================================

#[test]
fn test_is_enabled_default() {
    let connector = MirrorCheckpointConnector::new();
    // No config, should default to true
    assert!(connector.is_enabled());
}

#[test]
fn test_is_enabled_with_config_disabled() {
    let mut connector = MirrorCheckpointConnector::new();
    let props = create_config_with_checkpoints_disabled();
    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(props);

    assert!(!connector.is_enabled());
}

#[test]
fn test_is_enabled_with_config_enabled() {
    let mut connector = MirrorCheckpointConnector::new();
    let props = create_config_with_checkpoints_enabled();
    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(props);

    assert!(connector.is_enabled());
}

#[test]
fn test_connector_debug_format() {
    let connector = MirrorCheckpointConnector::new();
    let debug_str = format!("{:?}", connector);

    // Debug output should contain key fields
    assert!(debug_str.contains("MirrorCheckpointConnector"));
    assert!(debug_str.contains("started"));
    assert!(debug_str.contains("initial_groups_loaded"));
}

#[test]
fn test_config_def_available() {
    let connector = MirrorCheckpointConnector::new();
    let config_def = connector.config();

    // ConfigDef should be available
    let defs = config_def.config_def();
    assert!(!defs.is_empty());

    // Should have emit.checkpoints.enabled definition
    assert!(defs.contains_key(EMIT_CHECKPOINTS_ENABLED_CONFIG));
}

// ============================================================================
// Edge Cases Tests
// ============================================================================

#[test]
fn test_empty_config_defaults() {
    let mut connector = MirrorCheckpointConnector::new();

    // Start with minimal config (just aliases)
    let mut props = HashMap::new();
    props.insert(
        SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
        "source".to_string(),
    );
    props.insert(
        TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
        "target".to_string(),
    );

    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(props);

    // Should use defaults for other settings
    assert!(connector.is_started());
    assert!(connector.is_enabled()); // Default enabled
}

#[test]
fn test_known_consumer_groups_access() {
    let mut connector = MirrorCheckpointConnector::new();
    connector.add_known_consumer_group("group1".to_string());
    connector.add_known_consumer_group("group2".to_string());

    // Access known groups
    let groups = connector.known_consumer_groups();
    assert_eq!(groups.len(), 2);
    assert!(groups.contains("group1"));
    assert!(groups.contains("group2"));
}

#[test]
fn test_admin_client_access() {
    let source_admin = Arc::new(MockAdmin::new());
    let target_admin = Arc::new(MockAdmin::new());

    let connector =
        MirrorCheckpointConnector::with_admin_clients(source_admin.clone(), target_admin.clone());

    // Should be able to access admin clients
    let src = connector.source_admin();
    let tgt = connector.target_admin();

    assert!(Arc::ptr_eq(&src, &source_admin));
    assert!(Arc::ptr_eq(&tgt, &target_admin));
}

#[test]
fn test_multiple_start_stop_cycles() {
    let mut connector = MirrorCheckpointConnector::new();

    // First cycle
    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(create_basic_config());
    assert!(connector.is_started());
    connector.stop();
    assert!(!connector.is_started());

    // Second cycle
    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(create_basic_config());
    assert!(connector.is_started());
    connector.stop();
    assert!(!connector.is_started());
}

#[test]
fn test_filter_configuration_after_start() {
    let mut connector = MirrorCheckpointConnector::new();

    let mut props = create_basic_config();
    props.insert("groups".to_string(), "prod-.*".to_string());
    props.insert("topics".to_string(), "prod-.*".to_string());

    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(props);

    // Filters should be configured
    assert!(connector.group_filter().is_some());
    assert!(connector.topic_filter().is_some());

    // Verify filters work correctly
    let group_filter = connector.group_filter().unwrap();
    assert!(group_filter.should_replicate_group("prod-consumer-group"));
    assert!(!group_filter.should_replicate_group("test-consumer-group"));

    let topic_filter = connector.topic_filter().unwrap();
    assert!(topic_filter.should_replicate_topic("prod-topic"));
    assert!(!topic_filter.should_replicate_topic("test-topic"));
}

#[test]
fn test_config_access_after_start() {
    let mut connector = MirrorCheckpointConnector::new();
    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(create_basic_config());

    // Should be able to access config
    let config = connector.config_ref();
    assert!(config.is_some());

    // Config should have correct cluster aliases
    let cfg = config.unwrap();
    assert_eq!(cfg.source_cluster_alias(), "backup");
    assert_eq!(cfg.target_cluster_alias(), "primary");
}
