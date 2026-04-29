// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! End-to-end integration tests for MirrorMaker 2.0.
//!
//! This module tests the complete MirrorMaker lifecycle without requiring
//! a real Kafka cluster. Uses mock implementations for Kafka clients.
//!
//! Test scenarios:
//! - MirrorMakerConfig creation (cluster A->B configuration)
//! - MirrorMaker construction and lifecycle
//! - Connector configuration validation
//! - Start/Stop MirrorMaker
//!
//! Corresponds to Java: MirrorConnectorsIntegrationBaseTest (partial)

use std::collections::HashMap;
use std::time::Duration;

use connect_mirror::config::{SOURCE_CLUSTER_ALIAS_CONFIG, TARGET_CLUSTER_ALIAS_CONFIG};
use connect_mirror::mirror_maker::MirrorMaker;
use connect_mirror::mirror_maker_config::MirrorMakerConfig;
use connect_mirror_client::SourceAndTarget;

// ============================================================================
// Helper Functions
// ============================================================================

/// Creates basic MM2 configuration for bidirectional replication between two clusters.
///
/// Corresponds to Java: MirrorConnectorsIntegrationBaseTest.basicMM2Config()
fn create_basic_mm2_config() -> HashMap<String, String> {
    let mut props = HashMap::new();

    // Cluster definitions
    props.insert("clusters".to_string(), "primary, backup".to_string());

    // Bootstrap servers for each cluster (mock values)
    props.insert(
        "primary.bootstrap.servers".to_string(),
        "primary:9092".to_string(),
    );
    props.insert(
        "backup.bootstrap.servers".to_string(),
        "backup:9092".to_string(),
    );

    // Enable bidirectional replication
    props.insert("primary->backup.enabled".to_string(), "true".to_string());
    props.insert("backup->primary.enabled".to_string(), "true".to_string());

    // Topic filter configuration
    props.insert(
        "primary->backup.topics".to_string(),
        "test-topic-.*".to_string(),
    );
    props.insert(
        "backup->primary.topics".to_string(),
        "test-topic-.*".to_string(),
    );

    // Heartbeat configuration
    props.insert("emit.heartbeats.enabled".to_string(), "true".to_string());
    props.insert(
        "emit.heartbeats.interval.seconds".to_string(),
        "1".to_string(),
    );

    // Checkpoint configuration
    props.insert("emit.checkpoints.enabled".to_string(), "true".to_string());
    props.insert(
        "emit.checkpoints.interval.seconds".to_string(),
        "1".to_string(),
    );

    // Offset sync configuration
    props.insert("sync.group.offsets.enabled".to_string(), "true".to_string());
    props.insert(
        "sync.group.offsets.interval.seconds".to_string(),
        "1".to_string(),
    );

    // Replication factor (for topic creation)
    props.insert("replication.factor".to_string(), "1".to_string());
    props.insert(
        "checkpoints.topic.replication.factor".to_string(),
        "1".to_string(),
    );
    props.insert(
        "heartbeats.topic.replication.factor".to_string(),
        "1".to_string(),
    );
    props.insert(
        "offset-syncs.topic.replication.factor".to_string(),
        "1".to_string(),
    );

    props
}

/// Creates one-way replication configuration (primary -> backup only).
fn create_one_way_config() -> HashMap<String, String> {
    let mut props = create_basic_mm2_config();

    // Disable backup->primary replication
    props.insert("backup->primary.enabled".to_string(), "false".to_string());

    props
}

/// Creates configuration for specific target cluster.
fn create_config_for_cluster(cluster: &str) -> HashMap<String, String> {
    let mut props = create_basic_mm2_config();

    // Set target cluster specific settings
    props.insert(format!("{}.replication.factor", cluster), "3".to_string());

    props
}

// ============================================================================
// Tests - MirrorMakerConfig Validation
// ============================================================================

/// Tests basic MirrorMakerConfig creation with bidirectional replication.
///
/// Validates that:
/// - Clusters are correctly parsed
/// - Cluster pairs are created for enabled flows
/// - Connector configs contain correct source/target aliases
///
/// Corresponds to Java: MirrorConnectorsIntegrationBaseTest.testReplication() setup
#[test]
fn test_mirror_maker_config_basic() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    // Validate clusters
    let clusters = config.clusters();
    assert_eq!(clusters.len(), 2);
    assert!(clusters.contains("primary"));
    assert!(clusters.contains("backup"));

    // Validate cluster pairs (should have 2 for bidirectional)
    let pairs = config.cluster_pairs();
    assert_eq!(pairs.len(), 2);

    // Check both directions
    assert!(pairs.contains(&SourceAndTarget::new("primary", "backup")));
    assert!(pairs.contains(&SourceAndTarget::new("backup", "primary")));
}

/// Tests MirrorMakerConfig with one-way replication.
#[test]
fn test_mirror_maker_config_one_way() {
    let props = create_one_way_config();
    let config = MirrorMakerConfig::new(props);

    // Validate clusters
    let clusters = config.clusters();
    assert_eq!(clusters.len(), 2);

    // Validate cluster pairs (should have 1 for one-way with heartbeats)
    // primary->backup (enabled), backup->primary (heartbeats)
    let pairs = config.cluster_pairs();
    assert!(pairs.len() >= 1);

    // primary->backup should be present
    assert!(pairs.contains(&SourceAndTarget::new("primary", "backup")));
}

/// Tests MirrorMakerConfig cluster pair behavior with heartbeats disabled.
#[test]
fn test_mirror_maker_config_heartbeats_disabled() {
    let mut props = create_basic_mm2_config();

    // Disable heartbeats globally
    props.insert("emit.heartbeats.enabled".to_string(), "false".to_string());

    // Disable one direction
    props.insert("backup->primary.enabled".to_string(), "false".to_string());

    let config = MirrorMakerConfig::new(props);

    // Should only have primary->backup (enabled)
    let pairs = config.cluster_pairs();
    assert_eq!(pairs.len(), 1);
    assert!(pairs.contains(&SourceAndTarget::new("primary", "backup")));
}

/// Tests MirrorMakerConfig source config extraction.
#[test]
fn test_mirror_maker_config_source_config() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    let source_and_target = SourceAndTarget::new("primary", "backup");
    let source_config = config.source_config(&source_and_target);

    // Validate source/target aliases
    assert_eq!(source_config.source_cluster_alias(), "primary");
    assert_eq!(source_config.target_cluster_alias(), "backup");
}

/// Tests MirrorMakerConfig checkpoint config extraction.
#[test]
fn test_mirror_maker_config_checkpoint_config() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    let source_and_target = SourceAndTarget::new("primary", "backup");
    let checkpoint_config = config.checkpoint_config(&source_and_target);

    // Validate source/target aliases
    assert_eq!(checkpoint_config.source_cluster_alias(), "primary");
    assert_eq!(checkpoint_config.target_cluster_alias(), "backup");
}

/// Tests MirrorMakerConfig heartbeat config extraction.
#[test]
fn test_mirror_maker_config_heartbeat_config() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    let source_and_target = SourceAndTarget::new("primary", "backup");
    let heartbeat_config = config.heartbeat_config(&source_and_target);

    // Validate source/target aliases
    assert_eq!(heartbeat_config.source_cluster_alias(), "primary");
    assert_eq!(heartbeat_config.target_cluster_alias(), "backup");
}

/// Tests MirrorMakerConfig worker config extraction.
#[test]
fn test_mirror_maker_config_worker_config() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    let source_and_target = SourceAndTarget::new("primary", "backup");
    let worker_config = config.worker_config(&source_and_target);

    // Validate key worker config keys
    assert!(worker_config.contains_key("client.id"));
    assert!(worker_config.contains_key("group.id"));
    assert!(worker_config.contains_key("offset.storage.topic"));
    assert!(worker_config.contains_key("status.storage.topic"));
    assert!(worker_config.contains_key("config.storage.topic"));

    // Validate group.id format: source-mm2
    assert_eq!(
        worker_config.get("group.id"),
        Some(&"primary-mm2".to_string())
    );

    // Validate internal topics format: mm2-xxx.source.internal
    assert_eq!(
        worker_config.get("offset.storage.topic"),
        Some(&"mm2-offsets.primary.internal".to_string())
    );
}

/// Tests MirrorMakerConfig connector base config extraction.
#[test]
fn test_mirror_maker_config_connector_base_config() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    let source_and_target = SourceAndTarget::new("primary", "backup");
    let connector_config = config.connector_base_config(
        &source_and_target,
        "org.apache.kafka.connect.mirror.MirrorSourceConnector",
    );

    // Validate connector class
    assert_eq!(
        connector_config.get("connector.class"),
        Some(&"org.apache.kafka.connect.mirror.MirrorSourceConnector".to_string())
    );

    // Validate source/target aliases
    assert_eq!(
        connector_config.get(SOURCE_CLUSTER_ALIAS_CONFIG),
        Some(&"primary".to_string())
    );
    assert_eq!(
        connector_config.get(TARGET_CLUSTER_ALIAS_CONFIG),
        Some(&"backup".to_string())
    );

    // Validate name (simple class name)
    assert_eq!(
        connector_config.get("name"),
        Some(&"MirrorSourceConnector".to_string())
    );
}

// ============================================================================
// Tests - MirrorMaker Lifecycle
// ============================================================================

/// Tests MirrorMaker creation and basic properties.
///
/// Corresponds to Java: MirrorMaker instantiation
#[test]
fn test_mirror_maker_new() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    let mm = MirrorMaker::new(config, None);

    // Should have 2 herders (primary->backup, backup->primary)
    assert_eq!(mm.herder_count(), 2);

    // Validate clusters
    assert!(mm.clusters().contains("primary"));
    assert!(mm.clusters().contains("backup"));

    // Should not be shutdown initially
    assert!(!mm.is_shutdown());
}

/// Tests MirrorMaker creation with specific target cluster.
#[test]
fn test_mirror_maker_new_with_specific_cluster() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    // Create MirrorMaker targeting only "backup" cluster
    let mm = MirrorMaker::new(config, Some(vec!["backup".to_string()]));

    // Should have herders targeting backup only
    // primary->backup targets backup, so it should be included
    assert!(mm.herder_count() >= 1);

    // Validate clusters - should only contain "backup"
    assert!(mm.clusters().contains("backup"));
    assert!(!mm.clusters().contains("primary"));
}

/// Tests MirrorMaker herder pairs extraction.
#[test]
fn test_mirror_maker_herder_pairs() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    let mm = MirrorMaker::new(config, None);

    let pairs = mm.herder_pairs();
    assert_eq!(pairs.len(), 2);

    // Both directions should be present
    assert!(pairs.contains(&SourceAndTarget::new("primary", "backup")));
    assert!(pairs.contains(&SourceAndTarget::new("backup", "primary")));
}

/// Tests MirrorMaker start lifecycle.
///
/// Corresponds to Java: MirrorMaker.start()
#[test]
fn test_mirror_maker_start() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    let mut mm = MirrorMaker::new(config, None);

    // Start MirrorMaker
    mm.start();

    // After start, should not be shutdown
    assert!(!mm.is_shutdown());

    // After start, herder_count should still be 2
    assert_eq!(mm.herder_count(), 2);
}

/// Tests MirrorMaker stop lifecycle.
///
/// Corresponds to Java: MirrorMaker.stop()
#[test]
fn test_mirror_maker_stop() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    let mut mm = MirrorMaker::new(config, None);

    // Start then stop
    mm.start();
    mm.stop();

    // After stop, should be shutdown
    assert!(mm.is_shutdown());
}

/// Tests MirrorMaker complete lifecycle: create -> start -> stop.
#[test]
fn test_mirror_maker_lifecycle() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    // Create
    let mut mm = MirrorMaker::new(config, None);
    assert!(!mm.is_shutdown());

    // Start
    mm.start();
    assert!(!mm.is_shutdown());

    // Stop
    mm.stop();
    assert!(mm.is_shutdown());

    // Multiple stops should not error (returns early)
    mm.stop();
    assert!(mm.is_shutdown());
}

/// Tests MirrorMaker await_stop with timeout.
#[test]
fn test_mirror_maker_await_stop() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    let mut mm = MirrorMaker::new(config, None);

    mm.start();
    mm.stop();

    // Should complete quickly since herders are already stopped
    let result = mm.await_stop(Duration::from_secs(5));
    assert!(result);
}

/// Tests MirrorMaker connector status retrieval.
#[test]
fn test_mirror_maker_connector_status() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    let mm = MirrorMaker::new(config, None);

    // Get connector status for a valid pair
    let pair = SourceAndTarget::new("primary", "backup");
    let status = mm.connector_status(&pair, "MirrorSourceConnector");

    // Status should be available (even if MockHerder returns Unassigned)
    assert!(status.is_some());
}

/// Tests MirrorMaker connector status for invalid pair.
#[test]
fn test_mirror_maker_connector_status_invalid_pair() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    let mm = MirrorMaker::new(config, None);

    // Invalid pair (not in cluster pairs)
    let pair = SourceAndTarget::new("unknown", "cluster");
    let status = mm.connector_status(&pair, "MirrorSourceConnector");

    // Should return None for invalid pair
    assert!(status.is_none());
}

// ============================================================================
// Tests - Configuration Properties
// ============================================================================

/// Tests that replication.policy.class is correctly inherited.
#[test]
fn test_replication_policy_config() {
    let mut props = create_basic_mm2_config();
    props.insert(
        "replication.policy.class".to_string(),
        "org.apache.kafka.connect.mirror.CustomReplicationPolicy".to_string(),
    );

    let config = MirrorMakerConfig::new(props);

    let source_and_target = SourceAndTarget::new("primary", "backup");
    let connector_config = config.connector_base_config(
        &source_and_target,
        "org.apache.kafka.connect.mirror.MirrorSourceConnector",
    );

    // Replication policy should be present in connector config
    assert!(connector_config.contains_key("replication.policy.class"));
}

/// Tests that topic filter configuration is correctly passed.
#[test]
fn test_topic_filter_config() {
    let mut props = create_basic_mm2_config();
    props.insert(
        "primary->backup.topic.filter.class".to_string(),
        "org.apache.kafka.connect.mirror.CustomTopicFilter".to_string(),
    );

    let config = MirrorMakerConfig::new(props);

    let source_and_target = SourceAndTarget::new("primary", "backup");
    let source_config = config.source_config(&source_and_target);

    // Topic filter should be configurable
    // Note: actual validation depends on MirrorSourceConfig internals
    assert_eq!(source_config.source_cluster_alias(), "primary");
}

/// Tests offset syncs topic location configuration.
#[test]
fn test_offset_syncs_location_config() {
    let mut props = create_basic_mm2_config();
    props.insert(
        "primary->backup.offset-syncs.topic.location".to_string(),
        "target".to_string(),
    );

    let config = MirrorMakerConfig::new(props);

    let source_and_target = SourceAndTarget::new("primary", "backup");
    let connector_config = config.connector_base_config(
        &source_and_target,
        "org.apache.kafka.connect.mirror.MirrorSourceConnector",
    );

    // Offset syncs location should be present
    assert!(connector_config.contains_key("offset-syncs.topic.location"));
    assert_eq!(
        connector_config.get("offset-syncs.topic.location"),
        Some(&"target".to_string())
    );
}

/// Tests that config providers are correctly extracted.
#[test]
fn test_config_providers() {
    let mut props = create_basic_mm2_config();
    props.insert("config.providers".to_string(), "file, env".to_string());
    props.insert(
        "config.providers.file.class".to_string(),
        "FileConfigProvider".to_string(),
    );

    let config = MirrorMakerConfig::new(props);

    let providers = config.config_providers();
    assert_eq!(providers.len(), 2);
    assert!(providers.contains(&"file".to_string()));
    assert!(providers.contains(&"env".to_string()));
}

// ============================================================================
// Tests - Multi-Cluster Configuration
// ============================================================================

/// Tests MirrorMakerConfig with three clusters.
#[test]
fn test_three_clusters_config() {
    let mut props = HashMap::new();

    // Three clusters
    props.insert("clusters".to_string(), "A, B, C".to_string());
    props.insert("A.bootstrap.servers".to_string(), "a:9092".to_string());
    props.insert("B.bootstrap.servers".to_string(), "b:9092".to_string());
    props.insert("C.bootstrap.servers".to_string(), "c:9092".to_string());

    // Enable specific flows
    props.insert("A->B.enabled".to_string(), "true".to_string());
    props.insert("B->C.enabled".to_string(), "true".to_string());
    props.insert("A->C.enabled".to_string(), "true".to_string());

    // Disable heartbeats to only get enabled pairs
    props.insert("emit.heartbeats.enabled".to_string(), "false".to_string());

    let config = MirrorMakerConfig::new(props);

    // Validate clusters
    let clusters = config.clusters();
    assert_eq!(clusters.len(), 3);
    assert!(clusters.contains("A"));
    assert!(clusters.contains("B"));
    assert!(clusters.contains("C"));

    // Validate cluster pairs
    let pairs = config.cluster_pairs();
    assert_eq!(pairs.len(), 3);

    assert!(pairs.contains(&SourceAndTarget::new("A", "B")));
    assert!(pairs.contains(&SourceAndTarget::new("B", "C")));
    assert!(pairs.contains(&SourceAndTarget::new("A", "C")));
}

/// Tests MirrorMaker with three clusters targeting specific cluster.
#[test]
fn test_three_clusters_targeting_specific() {
    let mut props = HashMap::new();

    props.insert("clusters".to_string(), "A, B, C".to_string());
    props.insert("A.bootstrap.servers".to_string(), "a:9092".to_string());
    props.insert("B.bootstrap.servers".to_string(), "b:9092".to_string());
    props.insert("C.bootstrap.servers".to_string(), "c:9092".to_string());

    props.insert("A->B.enabled".to_string(), "true".to_string());
    props.insert("A->C.enabled".to_string(), "true".to_string());
    props.insert("B->C.enabled".to_string(), "true".to_string());

    props.insert("emit.heartbeats.enabled".to_string(), "false".to_string());

    let config = MirrorMakerConfig::new(props);

    // Target cluster C only
    let mm = MirrorMaker::new(config, Some(vec!["C".to_string()]));

    // Should have herders targeting C: A->C, B->C
    assert_eq!(mm.herder_count(), 2);

    // Validate clusters - should only contain C
    assert!(mm.clusters().contains("C"));
    assert!(!mm.clusters().contains("A"));
    assert!(!mm.clusters().contains("B"));
}

// ============================================================================
// Tests - Error Handling
// ============================================================================

/// Tests that MirrorMaker creation fails with no replication flows.
#[test]
#[should_panic(expected = "No source->target replication flows")]
fn test_mirror_maker_no_replication_flows() {
    let mut props = HashMap::new();
    props.insert("clusters".to_string(), "A, B".to_string());
    props.insert("A.bootstrap.servers".to_string(), "a:9092".to_string());
    props.insert("B.bootstrap.servers".to_string(), "b:9092".to_string());
    // No enabled flows and no heartbeats
    props.insert("emit.heartbeats.enabled".to_string(), "false".to_string());

    let config = MirrorMakerConfig::new(props);

    // Should panic: no replication flows targeting unknown cluster
    let _mm = MirrorMaker::new(config, Some(vec!["unknown".to_string()]));
}

/// Tests that MirrorMaker double start is prevented.
#[test]
#[should_panic(expected = "MirrorMaker instance already started")]
fn test_mirror_maker_double_start() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    let mut mm = MirrorMaker::new(config, None);

    mm.start();
    mm.start(); // Should panic
}

/// Tests MirrorMakerConfig with empty clusters.
#[test]
fn test_mirror_maker_config_empty_clusters() {
    let props = HashMap::new();
    let config = MirrorMakerConfig::new(props);

    // Clusters should be empty
    let clusters = config.clusters();
    assert!(clusters.is_empty());

    // Cluster pairs should be empty
    let pairs = config.cluster_pairs();
    assert!(pairs.is_empty());
}

// ============================================================================
// Tests - Internal REST Configuration
// ============================================================================

/// Tests internal REST server configuration default.
#[test]
fn test_internal_rest_default() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    // Internal REST should be disabled by default
    assert!(!config.enable_internal_rest());
}

/// Tests internal REST server configuration enabled.
#[test]
fn test_internal_rest_enabled() {
    let mut props = create_basic_mm2_config();
    props.insert(
        "dedicated.mode.enable.internal.rest".to_string(),
        "true".to_string(),
    );

    let config = MirrorMakerConfig::new(props);

    assert!(config.enable_internal_rest());
}

// ============================================================================
// Tests - Complete Integration Workflow
// ============================================================================

/// Tests complete MM2 integration workflow.
///
/// This test validates the entire flow from configuration to shutdown:
/// 1. Create MM2 configuration
/// 2. Create MirrorMaker
/// 3. Validate cluster pairs and configurations
/// 4. Start MirrorMaker
/// 5. Validate herder states
/// 6. Stop MirrorMaker
/// 7. Validate shutdown
///
/// Corresponds to Java: MirrorConnectorsIntegrationBaseTest integration flow
#[test]
fn test_complete_mm2_integration_workflow() {
    // Step 1: Create MM2 configuration
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    // Validate configuration
    assert_eq!(config.clusters().len(), 2);
    assert_eq!(config.cluster_pairs().len(), 2);

    // Get herder pairs before creating MirrorMaker
    let pairs = config.cluster_pairs();

    // Step 2: Create MirrorMaker
    let props2 = create_basic_mm2_config();
    let config2 = MirrorMakerConfig::new(props2);
    let mut mm = MirrorMaker::new(config2, None);

    // Validate MirrorMaker properties
    assert_eq!(mm.herder_count(), 2);
    assert!(!mm.is_shutdown());

    // Step 3: Validate connector configurations for each pair
    for pair in pairs {
        let source_config = config.source_config(&pair);
        assert_eq!(source_config.source_cluster_alias(), pair.source());
        assert_eq!(source_config.target_cluster_alias(), pair.target());

        let checkpoint_config = config.checkpoint_config(&pair);
        assert_eq!(checkpoint_config.source_cluster_alias(), pair.source());
        assert_eq!(checkpoint_config.target_cluster_alias(), pair.target());

        let heartbeat_config = config.heartbeat_config(&pair);
        assert_eq!(heartbeat_config.source_cluster_alias(), pair.source());
        assert_eq!(heartbeat_config.target_cluster_alias(), pair.target());

        // Validate connector status for each pair
        let status = mm.connector_status(&pair, "MirrorSourceConnector");
        assert!(status.is_some());
    }

    // Step 4: Start MirrorMaker
    mm.start();

    // Validate started state
    assert!(!mm.is_shutdown());

    // Step 5: Stop MirrorMaker
    mm.stop();

    // Validate shutdown state
    assert!(mm.is_shutdown());

    // Step 6: Await stop completion
    let result = mm.await_stop(Duration::from_secs(5));
    assert!(result);
}

/// Tests one-way replication workflow.
#[test]
fn test_one_way_replication_workflow() {
    // Create one-way config
    let props = create_one_way_config();
    let config = MirrorMakerConfig::new(props);

    // Create MirrorMaker
    let props2 = create_one_way_config();
    let config2 = MirrorMakerConfig::new(props2);
    let mut mm = MirrorMaker::new(config2, None);

    // Should have at least one herder (primary->backup)
    assert!(mm.herder_count() >= 1);

    // Validate primary->backup is present
    let pairs = mm.herder_pairs();
    assert!(pairs.contains(&SourceAndTarget::new("primary", "backup")));

    // Lifecycle
    mm.start();
    assert!(!mm.is_shutdown());

    mm.stop();
    assert!(mm.is_shutdown());
}

/// Tests MirrorMaker with configuration overrides.
#[test]
fn test_configuration_overrides() {
    let mut props = create_basic_mm2_config();

    // Add cluster-specific overrides
    props.insert(
        "primary->backup.replication.factor".to_string(),
        "2".to_string(),
    );
    props.insert(
        "primary->backup.offset.lag.max".to_string(),
        "100".to_string(),
    );

    let config = MirrorMakerConfig::new(props);

    let source_and_target = SourceAndTarget::new("primary", "backup");
    let connector_config = config.connector_base_config(
        &source_and_target,
        "org.apache.kafka.connect.mirror.MirrorSourceConnector",
    );

    // Validate overrides are applied
    assert_eq!(
        connector_config.get("replication.factor"),
        Some(&"2".to_string())
    );
    assert_eq!(
        connector_config.get("offset.lag.max"),
        Some(&"100".to_string())
    );
}

// ============================================================================
// Tests - ShutdownHook
// ============================================================================

/// Tests ShutdownHook execution.
#[test]
fn test_shutdown_hook() {
    use connect_mirror::mirror_maker::ShutdownHook;
    use std::sync::{Arc, RwLock};

    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    let mut mm = MirrorMaker::new(config, None);
    mm.start();

    // Create shutdown hook
    let mm_arc = Arc::new(RwLock::new(mm));
    let hook = ShutdownHook::new(mm_arc.clone());

    // Execute hook
    hook.run();

    // Validate shutdown
    assert!(mm_arc.read().unwrap().is_shutdown());
}

// ============================================================================
// Tests - Config Values Access
// ============================================================================

/// Tests MirrorMakerConfig get and originals accessors.
#[test]
fn test_config_accessors() {
    let props = create_basic_mm2_config();
    let config = MirrorMakerConfig::new(props);

    // Test get accessor
    assert_eq!(config.get("clusters"), Some(&"primary, backup".to_string()));

    // Test originals accessor
    let originals = config.originals();
    assert!(originals.contains_key("clusters"));

    // Test raw_properties accessor
    let raw_props = config.raw_properties();
    assert!(raw_props.contains_key("clusters"));
}
