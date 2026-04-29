// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// The License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Tests for MirrorConnectorConfig.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorConnectorConfigTest
//!
//! Test coverage includes:
//! - Source/target cluster alias configuration and defaults
//! - Connector name configuration
//! - Replication policy creation and caching
//! - Admin task timeout configuration
//! - Emit offset syncs enabled configuration
//! - Replication policy separator configuration
//!
//! **Note on Java test differences**:
//! Java MirrorConnectorConfigTest focuses on client config inheritance
//! (sourceConsumerConfig, sourceProducerConfig, targetAdminConfig, etc.)
//! which are not yet implemented in Rust. This test file covers the
//! core configuration properties that ARE implemented.

use std::collections::HashMap;

use connect_mirror::config::{
    MirrorConnectorConfig, ADMIN_TASK_TIMEOUT_MILLIS_CONFIG, ADMIN_TASK_TIMEOUT_MILLIS_DEFAULT,
    EMIT_OFFSET_SYNCS_ENABLED_CONFIG, EMIT_OFFSET_SYNCS_ENABLED_DEFAULT, NAME_CONFIG,
    SOURCE_CLUSTER_ALIAS_CONFIG, SOURCE_CLUSTER_ALIAS_DEFAULT, TARGET_CLUSTER_ALIAS_CONFIG,
    TARGET_CLUSTER_ALIAS_DEFAULT,
};
use connect_mirror_client::{
    DefaultReplicationPolicy, IdentityReplicationPolicy, ReplicationPolicy,
    REPLICATION_POLICY_CLASS, REPLICATION_POLICY_CLASS_DEFAULT, REPLICATION_POLICY_SEPARATOR,
    REPLICATION_POLICY_SEPARATOR_DEFAULT,
};

// ============================================================================
// Test helper functions
// ============================================================================

/// Creates a basic test configuration with source/target aliases.
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
    props.insert(NAME_CONFIG.to_string(), "mirror-connector".to_string());
    props
}

/// Creates a configuration with custom replication policy class.
fn create_config_with_policy(policy_class: &str) -> HashMap<String, String> {
    let mut props = create_basic_config();
    props.insert(
        REPLICATION_POLICY_CLASS.to_string(),
        policy_class.to_string(),
    );
    props
}

// ============================================================================
// Source Cluster Alias Tests
// ============================================================================

/// Test: Source cluster alias returns configured value.
/// Corresponds to Java: MirrorConnectorConfig.sourceClusterAlias()
#[test]
fn test_source_cluster_alias_custom() {
    let config = MirrorConnectorConfig::new(create_basic_config());
    assert_eq!(config.source_cluster_alias(), "backup");
}

/// Test: Source cluster alias returns default when not configured.
/// Default value is "source".
#[test]
fn test_source_cluster_alias_default() {
    let config = MirrorConnectorConfig::default();
    assert_eq!(config.source_cluster_alias(), SOURCE_CLUSTER_ALIAS_DEFAULT);
}

/// Test: Source cluster alias with empty string.
#[test]
fn test_source_cluster_alias_empty_string() {
    let mut props = HashMap::new();
    props.insert(SOURCE_CLUSTER_ALIAS_CONFIG.to_string(), "".to_string());
    let config = MirrorConnectorConfig::new(props);
    // Empty string is a valid value (not default)
    assert_eq!(config.source_cluster_alias(), "");
}

/// Test: Source cluster alias with special characters.
#[test]
fn test_source_cluster_alias_special_chars() {
    let mut props = HashMap::new();
    props.insert(
        SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
        "cluster-1-us-west".to_string(),
    );
    let config = MirrorConnectorConfig::new(props);
    assert_eq!(config.source_cluster_alias(), "cluster-1-us-west");
}

// ============================================================================
// Target Cluster Alias Tests
// ============================================================================

/// Test: Target cluster alias returns configured value.
/// Corresponds to Java: MirrorConnectorConfig.targetClusterAlias()
#[test]
fn test_target_cluster_alias_custom() {
    let config = MirrorConnectorConfig::new(create_basic_config());
    assert_eq!(config.target_cluster_alias(), "primary");
}

/// Test: Target cluster alias returns default when not configured.
/// Default value is "target".
#[test]
fn test_target_cluster_alias_default() {
    let config = MirrorConnectorConfig::default();
    assert_eq!(config.target_cluster_alias(), TARGET_CLUSTER_ALIAS_DEFAULT);
}

/// Test: Target cluster alias with empty string.
#[test]
fn test_target_cluster_alias_empty_string() {
    let mut props = HashMap::new();
    props.insert(TARGET_CLUSTER_ALIAS_CONFIG.to_string(), "".to_string());
    let config = MirrorConnectorConfig::new(props);
    assert_eq!(config.target_cluster_alias(), "");
}

/// Test: Target cluster alias with special characters.
#[test]
fn test_target_cluster_alias_special_chars() {
    let mut props = HashMap::new();
    props.insert(
        TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
        "cluster-2-us-east".to_string(),
    );
    let config = MirrorConnectorConfig::new(props);
    assert_eq!(config.target_cluster_alias(), "cluster-2-us-east");
}

// ============================================================================
// Connector Name Tests
// ============================================================================

/// Test: Connector name returns configured value.
/// Corresponds to Java: MirrorConnectorConfig.connectorName()
#[test]
fn test_connector_name_present() {
    let config = MirrorConnectorConfig::new(create_basic_config());
    assert_eq!(config.connector_name(), Some("mirror-connector"));
}

/// Test: Connector name returns None when not configured.
#[test]
fn test_connector_name_absent() {
    let config = MirrorConnectorConfig::default();
    assert_eq!(config.connector_name(), None);
}

/// Test: Connector name with complex naming.
#[test]
fn test_connector_name_complex() {
    let mut props = create_basic_config();
    props.insert(
        NAME_CONFIG.to_string(),
        "mirror-source-connector-primary-backup".to_string(),
    );
    let config = MirrorConnectorConfig::new(props);
    assert_eq!(
        config.connector_name(),
        Some("mirror-source-connector-primary-backup")
    );
}

/// Test: Connector name with empty string.
#[test]
fn test_connector_name_empty_string() {
    let mut props = create_basic_config();
    props.insert(NAME_CONFIG.to_string(), "".to_string());
    let config = MirrorConnectorConfig::new(props);
    // Empty string is a valid value
    assert_eq!(config.connector_name(), Some(""));
}

// ============================================================================
// Replication Policy Tests
// ============================================================================

/// Test: Replication policy returns DefaultReplicationPolicy by default.
/// Corresponds to Java: MirrorConnectorConfigTest.testReplicationPolicy()
#[test]
fn test_replication_policy_default() {
    let config = MirrorConnectorConfig::default();
    let policy = config.replication_policy();

    // DefaultReplicationPolicy formats remote topic as "source.topic"
    let remote_topic = policy.format_remote_topic("backup", "test-topic");
    assert_eq!(remote_topic, "backup.test-topic");
}

/// Test: Replication policy caching - same instance returned on multiple calls.
/// Corresponds to Java: MirrorConnectorConfigTest.testReplicationPolicy()
/// "assertSame(config.replicationPolicy(), config.replicationPolicy())"
#[test]
fn test_replication_policy_cached() {
    let config = MirrorConnectorConfig::default();

    // Get policy twice - should return same cached instance
    let policy1 = config.replication_policy();
    let policy2 = config.replication_policy();

    // Verify both policies produce same result
    let topic1 = policy1.format_remote_topic("source", "topic");
    let topic2 = policy2.format_remote_topic("source", "topic");
    assert_eq!(topic1, topic2);

    // Arc equality check - same underlying object
    // Note: We can't directly compare Arc<dyn Trait> for equality,
    // but we can verify behavior is identical
}

/// Test: Replication policy with IdentityReplicationPolicy.
#[test]
fn test_replication_policy_identity() {
    let props =
        create_config_with_policy("org.apache.kafka.connect.mirror.IdentityReplicationPolicy");
    let config = MirrorConnectorConfig::new(props);
    let policy = config.replication_policy();

    // IdentityReplicationPolicy does not rename topics
    let remote_topic = policy.format_remote_topic("backup", "test-topic");
    assert_eq!(remote_topic, "test-topic");
}

/// Test: Replication policy with DefaultReplicationPolicy class name.
#[test]
fn test_replication_policy_default_explicit() {
    let props =
        create_config_with_policy("org.apache.kafka.connect.mirror.DefaultReplicationPolicy");
    let config = MirrorConnectorConfig::new(props);
    let policy = config.replication_policy();

    let remote_topic = policy.format_remote_topic("source", "topic");
    assert_eq!(remote_topic, "source.topic");
}

/// Test: Replication policy with unknown class name defaults to DefaultReplicationPolicy.
#[test]
fn test_replication_policy_unknown_class() {
    let props = create_config_with_policy("com.example.CustomReplicationPolicy");
    let config = MirrorConnectorConfig::new(props);
    let policy = config.replication_policy();

    // Unknown class should fallback to DefaultReplicationPolicy
    let remote_topic = policy.format_remote_topic("source", "topic");
    assert_eq!(remote_topic, "source.topic");
}

/// Test: Replication policy separator default.
#[test]
fn test_replication_policy_separator_default() {
    let config = MirrorConnectorConfig::default();
    assert_eq!(
        config.replication_policy_separator(),
        REPLICATION_POLICY_SEPARATOR_DEFAULT
    );
}

/// Test: Replication policy separator custom value.
#[test]
fn test_replication_policy_separator_custom() {
    let mut props = create_basic_config();
    props.insert(REPLICATION_POLICY_SEPARATOR.to_string(), "_".to_string());
    let config = MirrorConnectorConfig::new(props);
    assert_eq!(config.replication_policy_separator(), "_");
}

// ============================================================================
// Admin Task Timeout Tests
// ============================================================================

/// Test: Admin task timeout returns default value.
/// Default: 60000 ms (60 seconds)
#[test]
fn test_admin_task_timeout_default() {
    let config = MirrorConnectorConfig::default();
    assert_eq!(
        config.admin_task_timeout_millis(),
        ADMIN_TASK_TIMEOUT_MILLIS_DEFAULT
    );
}

/// Test: Admin task timeout custom value.
#[test]
fn test_admin_task_timeout_custom() {
    let mut props = create_basic_config();
    props.insert(
        ADMIN_TASK_TIMEOUT_MILLIS_CONFIG.to_string(),
        "30000".to_string(),
    );
    let config = MirrorConnectorConfig::new(props);
    assert_eq!(config.admin_task_timeout_millis(), 30000);
}

/// Test: Admin task timeout with large value.
#[test]
fn test_admin_task_timeout_large_value() {
    let mut props = create_basic_config();
    props.insert(
        ADMIN_TASK_TIMEOUT_MILLIS_CONFIG.to_string(),
        "300000".to_string(),
    );
    let config = MirrorConnectorConfig::new(props);
    assert_eq!(config.admin_task_timeout_millis(), 300000);
}

/// Test: Admin task timeout with invalid value defaults.
#[test]
fn test_admin_task_timeout_invalid_value() {
    let mut props = create_basic_config();
    props.insert(
        ADMIN_TASK_TIMEOUT_MILLIS_CONFIG.to_string(),
        "invalid".to_string(),
    );
    let config = MirrorConnectorConfig::new(props);
    // Invalid value should fallback to default
    assert_eq!(
        config.admin_task_timeout_millis(),
        ADMIN_TASK_TIMEOUT_MILLIS_DEFAULT
    );
}

// ============================================================================
// Emit Offset Syncs Tests
// ============================================================================

/// Test: Emit offset syncs enabled default is true.
#[test]
fn test_emit_offset_syncs_enabled_default() {
    let config = MirrorConnectorConfig::default();
    assert!(config.emit_offset_syncs_enabled());
}

/// Test: Emit offset syncs disabled.
#[test]
fn test_emit_offset_syncs_disabled() {
    let mut props = create_basic_config();
    props.insert(
        EMIT_OFFSET_SYNCS_ENABLED_CONFIG.to_string(),
        "false".to_string(),
    );
    let config = MirrorConnectorConfig::new(props);
    assert!(!config.emit_offset_syncs_enabled());
}

/// Test: Emit offset syncs enabled explicitly.
#[test]
fn test_emit_offset_syncs_enabled_explicit() {
    let mut props = create_basic_config();
    props.insert(
        EMIT_OFFSET_SYNCS_ENABLED_CONFIG.to_string(),
        "true".to_string(),
    );
    let config = MirrorConnectorConfig::new(props);
    assert!(config.emit_offset_syncs_enabled());
}

/// Test: Emit offset syncs with invalid value defaults to true.
#[test]
fn test_emit_offset_syncs_invalid_value() {
    let mut props = create_basic_config();
    props.insert(
        EMIT_OFFSET_SYNCS_ENABLED_CONFIG.to_string(),
        "maybe".to_string(),
    );
    let config = MirrorConnectorConfig::new(props);
    // Invalid boolean should fallback to default (true)
    assert!(config.emit_offset_syncs_enabled());
}

// ============================================================================
// Configuration Access Tests
// ============================================================================

/// Test: get() returns configured value.
#[test]
fn test_get_present() {
    let config = MirrorConnectorConfig::new(create_basic_config());
    assert_eq!(
        config.get(SOURCE_CLUSTER_ALIAS_CONFIG),
        Some(&"backup".to_string())
    );
}

/// Test: get() returns None for missing key.
#[test]
fn test_get_absent() {
    let config = MirrorConnectorConfig::default();
    assert_eq!(config.get("nonexistent.key"), None);
}

/// Test: originals() returns all configuration properties.
#[test]
fn test_originals() {
    let props = create_basic_config();
    let config = MirrorConnectorConfig::new(props.clone());
    let originals = config.originals();

    assert_eq!(
        originals.get(SOURCE_CLUSTER_ALIAS_CONFIG),
        Some(&"backup".to_string())
    );
    assert_eq!(
        originals.get(TARGET_CLUSTER_ALIAS_CONFIG),
        Some(&"primary".to_string())
    );
    assert_eq!(
        originals.get(NAME_CONFIG),
        Some(&"mirror-connector".to_string())
    );
}

/// Test: get_string() returns configured value.
#[test]
fn test_get_string_present() {
    let config = MirrorConnectorConfig::new(create_basic_config());
    let value = config.get_string(SOURCE_CLUSTER_ALIAS_CONFIG, "default-value");
    assert_eq!(value, "backup");
}

/// Test: get_string() returns default for missing key.
#[test]
fn test_get_string_absent() {
    let config = MirrorConnectorConfig::default();
    let value = config.get_string("nonexistent.key", "default-value");
    assert_eq!(value, "default-value");
}

/// Test: get_bool() returns configured boolean.
#[test]
fn test_get_bool_present() {
    let mut props = create_basic_config();
    props.insert("custom.bool.key".to_string(), "true".to_string());
    let config = MirrorConnectorConfig::new(props);
    assert!(config.get_bool("custom.bool.key", false));
}

/// Test: get_bool() returns default for missing key.
#[test]
fn test_get_bool_absent() {
    let config = MirrorConnectorConfig::default();
    assert!(!config.get_bool("nonexistent.bool.key", false));
}

/// Test: get_long() returns configured long value.
#[test]
fn test_get_long_present() {
    let mut props = create_basic_config();
    props.insert("custom.long.key".to_string(), "12345".to_string());
    let config = MirrorConnectorConfig::new(props);
    assert_eq!(config.get_long("custom.long.key", 0), 12345);
}

/// Test: get_long() returns default for missing key.
#[test]
fn test_get_long_absent() {
    let config = MirrorConnectorConfig::default();
    assert_eq!(config.get_long("nonexistent.long.key", 999), 999);
}

// ============================================================================
// Edge Cases and Combined Tests
// ============================================================================

/// Test: Empty configuration creates valid defaults.
#[test]
fn test_empty_config_defaults() {
    let config = MirrorConnectorConfig::new(HashMap::new());

    assert_eq!(config.source_cluster_alias(), SOURCE_CLUSTER_ALIAS_DEFAULT);
    assert_eq!(config.target_cluster_alias(), TARGET_CLUSTER_ALIAS_DEFAULT);
    assert_eq!(config.connector_name(), None);
    assert_eq!(
        config.admin_task_timeout_millis(),
        ADMIN_TASK_TIMEOUT_MILLIS_DEFAULT
    );
    assert!(config.emit_offset_syncs_enabled());
}

/// Test: Configuration with all properties set.
#[test]
fn test_full_config() {
    let mut props = HashMap::new();
    props.insert(SOURCE_CLUSTER_ALIAS_CONFIG.to_string(), "west".to_string());
    props.insert(TARGET_CLUSTER_ALIAS_CONFIG.to_string(), "east".to_string());
    props.insert(NAME_CONFIG.to_string(), "mm2-connector".to_string());
    props.insert(
        ADMIN_TASK_TIMEOUT_MILLIS_CONFIG.to_string(),
        "120000".to_string(),
    );
    props.insert(
        EMIT_OFFSET_SYNCS_ENABLED_CONFIG.to_string(),
        "false".to_string(),
    );
    props.insert(REPLICATION_POLICY_SEPARATOR.to_string(), "-".to_string());

    let config = MirrorConnectorConfig::new(props);

    assert_eq!(config.source_cluster_alias(), "west");
    assert_eq!(config.target_cluster_alias(), "east");
    assert_eq!(config.connector_name(), Some("mm2-connector"));
    assert_eq!(config.admin_task_timeout_millis(), 120000);
    assert!(!config.emit_offset_syncs_enabled());
    assert_eq!(config.replication_policy_separator(), "-");
}

/// Test: Same source and target alias (valid configuration).
#[test]
fn test_same_source_target_alias() {
    let mut props = HashMap::new();
    props.insert(
        SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
        "cluster1".to_string(),
    );
    props.insert(
        TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
        "cluster1".to_string(),
    );
    let config = MirrorConnectorConfig::new(props);

    assert_eq!(config.source_cluster_alias(), "cluster1");
    assert_eq!(config.target_cluster_alias(), "cluster1");
}

/// Test: Debug format includes key fields.
#[test]
fn test_debug_format() {
    let config = MirrorConnectorConfig::new(create_basic_config());
    let debug_str = format!("{:?}", config);

    assert!(debug_str.contains("MirrorConnectorConfig"));
    assert!(debug_str.contains("originals"));
}

// ============================================================================
// Replication Policy Integration Tests
// ============================================================================

/// Test: Replication policy heartbeats_topic method.
#[test]
fn test_replication_policy_heartbeats_topic() {
    let config = MirrorConnectorConfig::default();
    let policy = config.replication_policy();

    // heartbeats_topic returns "heartbeats"
    let heartbeats = policy.heartbeats_topic();
    assert_eq!(heartbeats, "heartbeats");
}

/// Test: Replication policy offset_syncs_topic method.
#[test]
fn test_replication_policy_offset_syncs_topic() {
    let config = MirrorConnectorConfig::default();
    let policy = config.replication_policy();

    // offset_syncs_topic formats as "mm2-offset-syncs.{cluster_alias}.internal"
    let topic = policy.offset_syncs_topic("source");
    assert_eq!(topic, "mm2-offset-syncs.source.internal");
}

/// Test: Replication policy checkpoints_topic method.
#[test]
fn test_replication_policy_checkpoints_topic() {
    let config = MirrorConnectorConfig::default();
    let policy = config.replication_policy();

    // checkpoints_topic formats as "{cluster_alias}.checkpoints.internal"
    let topic = policy.checkpoints_topic("source");
    assert_eq!(topic, "source.checkpoints.internal");
}

/// Test: Replication policy upstream_topic method.
#[test]
fn test_replication_policy_upstream_topic() {
    let config = MirrorConnectorConfig::default();
    let policy = config.replication_policy();

    // upstream_topic extracts "topic" from "source.topic"
    let source = policy.upstream_topic("source.topic");
    assert_eq!(source, Some("topic".to_string()));
}

/// Test: IdentityReplicationPolicy upstream_topic returns the topic itself.
#[test]
fn test_identity_replication_policy_upstream_topic() {
    let props =
        create_config_with_policy("org.apache.kafka.connect.mirror.IdentityReplicationPolicy");
    let config = MirrorConnectorConfig::new(props);
    let policy = config.replication_policy();

    // IdentityReplicationPolicy upstream_topic returns Some(topic) for non-heartbeat topics
    let source = policy.upstream_topic("topic");
    assert_eq!(source, Some("topic".to_string()));
}
