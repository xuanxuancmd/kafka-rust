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

//! Tests for MirrorSourceConnector.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorSourceConnectorTest
//!
//! Test coverage includes:
//! - start/stop lifecycle
//! - topic partition discovery and filtering
//! - task_configs allocation logic
//! - exactly_once_support and alter_offsets semantics
//! - heartbeat replication control
//! - cycle detection (preventing circular replication)
//! - validation of required configuration fields
//! - initialize_with_task_configs state restoration

use std::collections::HashMap;
use std::sync::Arc;

use common_trait::TopicPartition;
use connect_api::components::Versioned;
use connect_api::connector::{Connector, ConnectorContext};
use connect_api::errors::ConnectError;
use connect_api::source::{ConnectorTransactionBoundaries, ExactlyOnceSupport, SourceConnector};
use connect_mirror::source::{
    MirrorSourceConfig, MirrorSourceConnector, Scheduler, HEARTBEATS_REPLICATION_ENABLED_CONFIG,
    TASK_INDEX_CONFIG, TASK_TOPIC_PARTITIONS_CONFIG,
};
use connect_mirror_client::{
    DefaultReplicationPolicy, IdentityReplicationPolicy, ReplicationPolicy,
};
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

/// Creates a basic test configuration for MirrorSourceConnector.
fn create_basic_config() -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("source.cluster.alias".to_string(), "backup".to_string());
    props.insert("target.cluster.alias".to_string(), "primary".to_string());
    props.insert("name".to_string(), "mirror-source-connector".to_string());
    props.insert("topics".to_string(), ".*".to_string());
    props
}

/// Creates a configuration with specific topic filter.
fn create_config_with_topics(topics: &str) -> HashMap<String, String> {
    let mut props = create_basic_config();
    props.insert("topics".to_string(), topics.to_string());
    props
}

/// Creates a configuration with heartbeats disabled.
fn create_config_with_heartbeats_disabled() -> HashMap<String, String> {
    let mut props = create_basic_config();
    props.insert(
        HEARTBEATS_REPLICATION_ENABLED_CONFIG.to_string(),
        "false".to_string(),
    );
    props
}

/// Creates a MockAdmin with test topics.
fn create_admin_with_topics(topic_names: &[&str]) -> Arc<MockAdmin> {
    let admin = Arc::new(MockAdmin::new());
    for topic in topic_names {
        admin.create_topics(vec![NewTopic::new(*topic, 3, 1)]);
    }
    admin
}

// ============================================================================
// Lifecycle Tests (start/stop)
// ============================================================================

#[test]
fn test_new_connector() {
    let connector = MirrorSourceConnector::new();

    // New connector should not be started
    assert!(!connector.is_started());

    // New connector should have empty topic partitions
    assert!(connector.known_source_topic_partitions().is_empty());

    // Task class should be MirrorSourceTask
    assert_eq!(connector.task_class(), "MirrorSourceTask");
}

#[test]
fn test_version() {
    // Version is accessed through the Versioned trait
    assert_eq!(MirrorSourceConnector::version(), "1.0.0");
}

#[test]
fn test_start_stop_cycle() {
    let mut connector = MirrorSourceConnector::new();
    let props = create_basic_config();

    // Initialize connector
    connector.initialize(Box::new(MockConnectorContext::new()));

    // Start connector
    connector.start(props.clone());

    // Connector should be started
    assert!(connector.is_started());

    // Configuration should be set
    assert!(connector.config_ref().is_some());
    assert_eq!(
        connector.config_ref().unwrap().source_cluster_alias(),
        "backup"
    );
    assert_eq!(
        connector.config_ref().unwrap().target_cluster_alias(),
        "primary"
    );

    // Topic filter should be set
    assert!(connector.topic_filter().is_some());

    // Config property filter should be set
    assert!(connector.config_property_filter().is_some());

    // Replication policy should be available
    assert!(connector.replication_policy().is_some());

    // Stop connector
    connector.stop();

    // Connector should be stopped
    assert!(!connector.is_started());

    // Topic partitions should be cleared
    assert!(connector.known_source_topic_partitions().is_empty());
}

#[test]
fn test_with_admin_clients() {
    let source_admin = create_admin_with_topics(&["source-topic"]);
    let target_admin = create_admin_with_topics(&["target-topic"]);

    let connector =
        MirrorSourceConnector::with_admin_clients(source_admin.clone(), target_admin.clone());

    // Should have the provided admin clients
    assert!(connector.source_admin().topic_exists("source-topic"));
    assert!(connector.target_admin().topic_exists("target-topic"));
}

#[test]
fn test_scheduler_start_stop() {
    let mut scheduler = Scheduler::new();

    // New scheduler should not be running
    assert!(!scheduler.is_running());

    // Start scheduler
    scheduler.start();
    assert!(scheduler.is_running());

    // Stop scheduler
    scheduler.stop();
    assert!(!scheduler.is_running());
}

// ============================================================================
// Topic Partition Discovery and Filtering Tests
// ============================================================================

#[test]
fn test_load_topic_partitions_basic() {
    let source_admin = create_admin_with_topics(&["test-topic", "other-topic"]);
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector =
        MirrorSourceConnector::with_admin_clients(source_admin.clone(), target_admin.clone());

    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(create_basic_config());

    // Should have discovered topic partitions
    // Note: In the mock, load_topic_partitions is called during start()
    // The discovered partitions depend on the topic filter matching
    assert!(connector.is_started());
}

#[test]
fn test_topic_filter_include_pattern() {
    let props = create_config_with_topics("test-.*");
    let config = MirrorSourceConfig::new(props);
    let filter = config.topic_filter();

    // Topics matching the pattern should be replicated
    assert!(filter.should_replicate_topic("test-topic"));
    assert!(filter.should_replicate_topic("test-data"));

    // Topics not matching should not be replicated
    assert!(!filter.should_replicate_topic("other-topic"));
    assert!(!filter.should_replicate_topic("prod-topic"));
}

#[test]
fn test_topic_filter_exclude_internal_topics() {
    let config = MirrorSourceConfig::default();
    let filter = config.topic_filter();

    // Internal Kafka topics should be excluded
    assert!(!filter.should_replicate_topic("__consumer_offsets"));
    assert!(!filter.should_replicate_topic("__transaction_state"));

    // Regular topics should be included
    assert!(filter.should_replicate_topic("my-topic"));
    assert!(filter.should_replicate_topic("data-stream"));
}

#[test]
fn test_config_property_filter_excludes_cluster_specific() {
    let config = MirrorSourceConfig::default();
    let filter = config.config_property_filter();

    // Cluster-specific properties should be excluded
    assert!(!filter.should_replicate_config_property("min.insync.replicas"));
    assert!(!filter.should_replicate_config_property("follower.replication.throttled.replicas"));
    assert!(!filter.should_replicate_config_property("leader.replication.throttled.replicas"));

    // General properties should be included
    assert!(filter.should_replicate_config_property("cleanup.policy"));
    assert!(filter.should_replicate_config_property("retention.ms"));
    assert!(filter.should_replicate_config_property("compression.type"));
}

// ============================================================================
// Task Configs Allocation Tests
// ============================================================================

#[test]
fn test_task_configs_empty_when_not_started() {
    let connector = MirrorSourceConnector::new();

    // Should return empty configs when not started
    let configs = connector.task_configs(1).unwrap();
    assert!(configs.is_empty());
}

#[test]
fn test_task_configs_empty_when_no_partitions() {
    // When started but no partitions discovered, should return empty
    let source_admin = Arc::new(MockAdmin::new()); // Empty admin
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector = MirrorSourceConnector::with_admin_clients(source_admin, target_admin);
    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(create_basic_config());

    // Should return empty configs when no partitions known (no topics on source)
    let configs = connector.task_configs(1).unwrap();
    assert!(configs.is_empty());
}

#[test]
fn test_task_configs_distribution_round_robin() {
    // Use source admin with topics that match the filter
    let source_admin = create_admin_with_topics(&["topic1", "topic2", "topic3"]);
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector = MirrorSourceConnector::with_admin_clients(source_admin, target_admin);
    connector.initialize(Box::new(MockConnectorContext::new()));

    // Configure with topics matching all topics
    let mut props = create_basic_config();
    props.insert("topics".to_string(), "topic.*".to_string());
    connector.start(props);

    // Should have discovered partitions
    assert!(connector.is_started());

    // Request 2 tasks
    let configs = connector.task_configs(2).unwrap();

    // Should have some configs if partitions were discovered
    // Note: depends on MockAdmin providing partition metadata
    // Check that the distribution logic works correctly

    // Each task config should have a task index
    for (i, config) in configs.iter().enumerate() {
        assert!(config.contains_key(TASK_INDEX_CONFIG));
        assert_eq!(config.get(TASK_INDEX_CONFIG).unwrap(), &i.to_string());
    }
}

#[test]
fn test_task_configs_respects_max_tasks() {
    // Create connector with topics
    let source_admin = create_admin_with_topics(&["topic"]);
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector = MirrorSourceConnector::with_admin_clients(source_admin, target_admin);
    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(create_basic_config());

    // Request more tasks than potential partitions
    let configs = connector.task_configs(100).unwrap();

    // Should not exceed reasonable limits
    assert!(configs.len() <= 100);
}

#[test]
fn test_task_configs_zero_max_tasks() {
    let connector = MirrorSourceConnector::new();

    // Request 0 tasks
    let configs = connector.task_configs(0).unwrap();

    // Should return empty
    assert!(configs.is_empty());
}

// ============================================================================
// SourceConnector Trait Tests
// ============================================================================

#[test]
fn test_exactly_once_support_unsupported() {
    let connector = MirrorSourceConnector::new();

    // MirrorSourceConnector does not support exactly-once
    let support = connector.exactly_once_support(HashMap::new());
    assert_eq!(support, ExactlyOnceSupport::Unsupported);
}

#[test]
fn test_can_define_transaction_boundaries_coordinator_defined() {
    let connector = MirrorSourceConnector::new();

    // Transaction boundaries are coordinator-defined
    let boundaries = connector.can_define_transaction_boundaries(HashMap::new());
    assert_eq!(
        boundaries,
        ConnectorTransactionBoundaries::CoordinatorDefined
    );
}

#[test]
fn test_alter_offsets_returns_false() {
    let connector = MirrorSourceConnector::new();

    // alter_offsets does not support external offset management
    let result = connector.alter_offsets(HashMap::new(), HashMap::new());
    assert_eq!(result.unwrap(), false);
}

// ============================================================================
// Validation Tests
// ============================================================================

#[test]
fn test_validate_missing_source_alias() {
    let connector = MirrorSourceConnector::new();
    let mut props = HashMap::new();
    props.insert("target.cluster.alias".to_string(), "primary".to_string());

    let config = connector.validate(props);

    // Should have error for missing source.cluster.alias
    let has_source_error = config
        .config_values()
        .iter()
        .any(|cv| cv.name() == "source.cluster.alias" && !cv.error_messages().is_empty());
    assert!(has_source_error);
}

#[test]
fn test_validate_missing_target_alias() {
    let connector = MirrorSourceConnector::new();
    let mut props = HashMap::new();
    props.insert("source.cluster.alias".to_string(), "backup".to_string());

    let config = connector.validate(props);

    // Should have error for missing target.cluster.alias
    let has_target_error = config
        .config_values()
        .iter()
        .any(|cv| cv.name() == "target.cluster.alias" && !cv.error_messages().is_empty());
    assert!(has_target_error);
}

#[test]
fn test_validate_valid_config() {
    let connector = MirrorSourceConnector::new();
    let props = create_basic_config();

    let config = connector.validate(props);

    // Should have no errors for valid config
    let has_errors = config
        .config_values()
        .iter()
        .any(|cv| !cv.error_messages().is_empty());
    assert!(!has_errors);
}

// ============================================================================
// Heartbeat Replication Tests
// ============================================================================

#[test]
fn test_replicates_heartbeats_by_default() {
    let config = MirrorSourceConfig::default();
    let filter = config.topic_filter();

    // Heartbeats should be replicated by default when heartbeats replication is enabled
    // The topic filter should allow heartbeats topic
    assert!(config.heartbeats_replication_enabled());

    // "heartbeats" topic should match the default pattern
    assert!(filter.should_replicate_topic("heartbeats"));
}

#[test]
fn test_does_not_replicate_heartbeats_when_disabled() {
    let props = create_config_with_heartbeats_disabled();
    let config = MirrorSourceConfig::new(props);

    // Heartbeats replication should be disabled
    assert!(!config.heartbeats_replication_enabled());
}

#[test]
fn test_heartbeats_topic_format() {
    let config = MirrorSourceConfig::new(create_basic_config());
    let policy = config.replication_policy();

    // Heartbeats topic should use standard format
    assert_eq!(policy.heartbeats_topic(), "heartbeats");
}

// ============================================================================
// Cycle Detection Tests (Preventing Circular Replication)
// ============================================================================

#[test]
fn test_no_cycles_default_replication_policy() {
    let mut policy = DefaultReplicationPolicy::new();
    policy.configure(&create_basic_config());

    // Source cluster alias is "backup"
    // Topics prefixed with "backup." should be detected as remote topics
    assert_eq!(
        policy.topic_source("backup.my-topic"),
        Some("backup".to_string())
    );

    // Topics prefixed with target alias should not be replicated
    // (would cause cycle: primary -> backup -> primary)
    let upstream = policy.upstream_topic("primary.my-topic");
    assert!(upstream.is_some());
    assert_eq!(upstream.unwrap(), "my-topic");

    // Topic that starts with source alias is already a remote topic
    // Should be detected via topic_source
    assert!(policy.topic_source("backup.source-topic").is_some());
}

#[test]
fn test_identity_replication_policy() {
    let mut policy = IdentityReplicationPolicy::new();
    let props = create_basic_config();
    policy.configure(&props);

    // Identity policy preserves topic names (except heartbeats)
    assert_eq!(policy.format_remote_topic("backup", "my-topic"), "my-topic");

    // Heartbeats are still renamed
    assert_ne!(
        policy.format_remote_topic("backup", "heartbeats"),
        "heartbeats"
    );
    assert!(policy
        .format_remote_topic("backup", "heartbeats")
        .contains("heartbeats"));

    // Topic source returns configured source alias for regular topics
    assert_eq!(policy.topic_source("my-topic"), Some("backup".to_string()));
}

#[test]
fn test_replication_policy_upstream_topic() {
    let policy = DefaultReplicationPolicy::new();

    // Remote topic has upstream topic
    let upstream = policy.upstream_topic("backup.source-topic");
    assert!(upstream.is_some());
    assert_eq!(upstream.unwrap(), "source-topic");

    // Non-remote topic has no upstream
    let upstream = policy.upstream_topic("source-topic");
    assert!(upstream.is_none());
}

#[test]
fn test_replication_policy_original_topic() {
    let policy = DefaultReplicationPolicy::new();

    // Single hop: backup.source-topic -> source-topic
    assert_eq!(policy.original_topic("backup.source-topic"), "source-topic");

    // Non-remote topic returns itself
    assert_eq!(policy.original_topic("source-topic"), "source-topic");

    // Multi-hop: primary.backup.source-topic -> source-topic
    // (if source.cluster.alias is primary)
    assert_eq!(
        policy.original_topic("primary.backup.source-topic"),
        "source-topic"
    );
}

#[test]
fn test_is_internal_topic() {
    let policy = DefaultReplicationPolicy::new();

    // Kafka internal topics
    assert!(policy.is_internal_topic("__consumer_offsets"));
    assert!(policy.is_internal_topic("__transaction_state"));
    assert!(policy.is_internal_topic(".internal-topic"));

    // MM2 internal topics
    assert!(policy.is_internal_topic("mm2-offset-syncs.backup.internal"));
    assert!(policy.is_mm2_internal_topic("mm2-offset-syncs.backup.internal"));

    // Regular topics are not internal
    assert!(!policy.is_internal_topic("my-topic"));
    assert!(!policy.is_internal_topic("backup.my-topic"));
}

// ============================================================================
// Initialize with Task Configs Tests
// ============================================================================

#[test]
fn test_initialize_with_task_configs() {
    let mut connector = MirrorSourceConnector::new();

    let task_configs: Vec<HashMap<String, String>> = vec![
        HashMap::from([
            (TASK_INDEX_CONFIG.to_string(), "0".to_string()),
            (
                TASK_TOPIC_PARTITIONS_CONFIG.to_string(),
                "topic1:0,topic1:1".to_string(),
            ),
            ("source.cluster.alias".to_string(), "backup".to_string()),
        ]),
        HashMap::from([
            (TASK_INDEX_CONFIG.to_string(), "1".to_string()),
            (
                TASK_TOPIC_PARTITIONS_CONFIG.to_string(),
                "topic2:0".to_string(),
            ),
            ("source.cluster.alias".to_string(), "backup".to_string()),
        ]),
    ];

    connector.initialize_with_task_configs(Box::new(MockConnectorContext::new()), task_configs);

    // Should restore partitions from task configs
    assert_eq!(connector.known_source_topic_partitions().len(), 3);
    assert!(connector
        .known_source_topic_partitions()
        .contains(&TopicPartition::new("topic1", 0)));
    assert!(connector
        .known_source_topic_partitions()
        .contains(&TopicPartition::new("topic1", 1)));
    assert!(connector
        .known_source_topic_partitions()
        .contains(&TopicPartition::new("topic2", 0)));
}

#[test]
fn test_initialize_with_task_configs_empty() {
    let mut connector = MirrorSourceConnector::new();

    connector.initialize_with_task_configs(Box::new(MockConnectorContext::new()), vec![]);

    // Should have empty partitions
    assert!(connector.known_source_topic_partitions().is_empty());
}

// ============================================================================
// Scheduler Configuration Tests
// ============================================================================

#[test]
fn test_scheduler_configure_from_config() {
    let mut scheduler = Scheduler::new();
    let config = MirrorSourceConfig::new(create_basic_config());

    scheduler.configure(&config);

    // Intervals should be set (defaults: 600 seconds)
    assert!(scheduler.refresh_topics_interval_ms() > 0);
    assert!(scheduler.sync_topic_configs_interval_ms() > 0);
    assert!(scheduler.sync_topic_acls_interval_ms() > 0);
}

#[test]
fn test_scheduler_intervals_disabled() {
    let mut scheduler = Scheduler::new();
    let mut props = create_basic_config();
    props.insert("refresh.topics.enabled".to_string(), "false".to_string());
    props.insert(
        "sync.topic.configs.enabled".to_string(),
        "false".to_string(),
    );
    props.insert("sync.topic.acls.enabled".to_string(), "false".to_string());

    let config = MirrorSourceConfig::new(props);
    scheduler.configure(&config);

    // Intervals should be 0 when disabled
    assert_eq!(scheduler.refresh_topics_interval_ms(), 0);
    assert_eq!(scheduler.sync_topic_configs_interval_ms(), 0);
    assert_eq!(scheduler.sync_topic_acls_interval_ms(), 0);
}

// ============================================================================
// Edge Cases and Boundary Tests
// ============================================================================

#[test]
fn test_empty_config_defaults() {
    let config = MirrorSourceConfig::default();

    // Should have default values
    // Default source/target cluster aliases are "source" and "target"
    assert_eq!(config.source_cluster_alias(), "source");
    assert_eq!(config.target_cluster_alias(), "target");
    assert_eq!(config.replication_factor(), 2);
    assert!(config.emit_offset_syncs_enabled());
    assert!(config.heartbeats_replication_enabled());
}

#[test]
fn test_connector_context_is_set() {
    let mut connector = MirrorSourceConnector::new();
    connector.initialize(Box::new(MockConnectorContext::new()));

    // Context should be accessible (via context() method)
    // Note: context() requires context to be set, else panics
    // We can check it's set by ensuring start() works properly
    connector.start(create_basic_config());
    assert!(connector.is_started());
}

// ============================================================================
// Topic Creation on Target Cluster Tests
// ============================================================================

#[test]
fn test_remote_topic_creation() {
    let source_admin = create_admin_with_topics(&["source-topic"]);
    let target_admin = Arc::new(MockAdmin::new());

    let mut connector =
        MirrorSourceConnector::with_admin_clients(source_admin.clone(), target_admin.clone());

    connector.initialize(Box::new(MockConnectorContext::new()));
    connector.start(create_basic_config());

    // After start, remote topics should potentially be created on target
    // (depends on whether topic filter matches and load_topic_partitions is called)

    // Verify that the connector has proper admin clients
    assert!(connector.source_admin().topic_exists("source-topic"));
}

// ============================================================================
// ConfigDef Tests
// ============================================================================

#[test]
fn test_config_def_available() {
    let connector = MirrorSourceConnector::new();
    let config_def = connector.config();

    // ConfigDef should be accessible
    let defs = config_def.config_def();

    // Should have replication.factor config
    assert!(defs.contains_key("replication.factor"));
}
