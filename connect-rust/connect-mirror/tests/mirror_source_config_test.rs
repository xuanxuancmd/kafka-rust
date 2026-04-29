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

//! Tests for MirrorSourceConfig.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorSourceConfigTest
//!
//! Test coverage includes:
//! - Topic matching with various patterns (empty, wildcard, list)
//! - Config property matching and exclusion
//! - Configuration property defaults and custom values
//! - Replication factor settings
//! - Interval settings for sync operations

use std::collections::HashMap;
use std::time::Duration;

use connect_mirror::config::{
    EMIT_OFFSET_SYNCS_ENABLED_CONFIG, SOURCE_CLUSTER_ALIAS_CONFIG, SOURCE_CLUSTER_ALIAS_DEFAULT,
    TARGET_CLUSTER_ALIAS_CONFIG, TARGET_CLUSTER_ALIAS_DEFAULT,
};
use connect_mirror::source::{
    ConfigPropertyFilter, DefaultConfigPropertyFilter, DefaultTopicFilter, MirrorSourceConfig,
    TopicFilter, CONFIG_PROPERTIES_EXCLUDE_CONFIG, CONFIG_PROPERTIES_EXCLUDE_DEFAULT,
    CONSUMER_POLL_TIMEOUT_MILLIS_CONFIG, CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT,
    HEARTBEATS_REPLICATION_ENABLED_CONFIG, OFFSET_LAG_MAX_CONFIG, OFFSET_LAG_MAX_DEFAULT,
    OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_CONFIG, OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_DEFAULT,
    REFRESH_TOPICS_ENABLED_CONFIG, REFRESH_TOPICS_INTERVAL_SECONDS_CONFIG,
    REFRESH_TOPICS_INTERVAL_SECONDS_DEFAULT, REPLICATION_FACTOR_CONFIG, REPLICATION_FACTOR_DEFAULT,
    SYNC_TOPIC_ACLS_ENABLED_CONFIG, SYNC_TOPIC_ACLS_INTERVAL_SECONDS_DEFAULT,
    SYNC_TOPIC_CONFIGS_ENABLED_CONFIG, SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_CONFIG,
    SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_DEFAULT, TOPICS_EXCLUDE_CONFIG, TOPICS_EXCLUDE_DEFAULT,
    TOPICS_INCLUDE_CONFIG, TOPICS_INCLUDE_DEFAULT,
};

// ============================================================================
// Test helper functions
// ============================================================================

/// Creates a basic test configuration for MirrorSourceConfig.
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
    props.insert("name".to_string(), "mirror-source-connector".to_string());
    props
}

/// Creates a configuration with specific topic pattern.
fn create_config_with_topics(topics: &str) -> HashMap<String, String> {
    let mut props = create_basic_config();
    props.insert(TOPICS_INCLUDE_CONFIG.to_string(), topics.to_string());
    props
}

/// Creates a configuration with specific config property exclude pattern.
fn create_config_with_config_exclude(exclude: &str) -> HashMap<String, String> {
    let mut props = create_basic_config();
    props.insert(
        CONFIG_PROPERTIES_EXCLUDE_CONFIG.to_string(),
        exclude.to_string(),
    );
    props
}

// ============================================================================
// testTopicMatching Tests - Topic filter behavior
// ============================================================================

/// Test: Empty topics config means no topics are selected.
/// Corresponds to Java: MirrorSourceConfigTest.testNoTopics()
#[test]
fn test_no_topics() {
    let mut props = create_basic_config();
    props.insert(TOPICS_INCLUDE_CONFIG.to_string(), "".to_string());

    let config = MirrorSourceConfig::new(props);
    let filter = config.topic_filter();

    // Empty pattern should not match any topic
    assert!(!filter.should_replicate_topic("test-topic"));
    assert!(!filter.should_replicate_topic("my-topic"));
    assert!(!filter.should_replicate_topic("any-topic"));
}

/// Test: Wildcard .* matches all topics.
/// Corresponds to Java: MirrorSourceConfigTest.testAllTopics()
#[test]
fn test_all_topics() {
    let props = create_config_with_topics(".*");

    let config = MirrorSourceConfig::new(props);
    let filter = config.topic_filter();

    // .* should match all topics
    assert!(filter.should_replicate_topic("test-topic"));
    assert!(filter.should_replicate_topic("my-topic"));
    assert!(filter.should_replicate_topic("any-topic"));
    assert!(filter.should_replicate_topic("production-data"));
}

/// Test: Comma-separated list of topics matches exactly those topics.
/// Corresponds to Java: MirrorSourceConfigTest.testListOfTopics()
#[test]
fn test_list_of_topics() {
    let props = create_config_with_topics("topic1,topic2,topic3");

    let config = MirrorSourceConfig::new(props);
    let filter = config.topic_filter();

    // Listed topics should match
    assert!(filter.should_replicate_topic("topic1"));
    assert!(filter.should_replicate_topic("topic2"));
    assert!(filter.should_replicate_topic("topic3"));

    // Non-listed topics should not match
    assert!(!filter.should_replicate_topic("topic4"));
    assert!(!filter.should_replicate_topic("other-topic"));
}

/// Test: Topic matching with regex prefix pattern.
/// Corresponds to Java: MirrorSourceConfigTest.testTopicMatching()
#[test]
fn test_topic_matching_prefix_pattern() {
    let props = create_config_with_topics("test.*");

    let config = MirrorSourceConfig::new(props);
    let filter = config.topic_filter();

    // Topics starting with 'test' should match
    assert!(filter.should_replicate_topic("test-topic"));
    assert!(filter.should_replicate_topic("test123"));
    assert!(filter.should_replicate_topic("testing"));

    // Other topics should not match
    assert!(!filter.should_replicate_topic("my-test"));
    assert!(!filter.should_replicate_topic("other-topic"));
}

/// Test: Topic matching combined with exclusion.
/// Corresponds to Java: MirrorSourceConfigTest.testTopicMatching()
#[test]
fn test_topic_matching_with_exclusion() {
    let mut props = create_config_with_topics("test.*");
    props.insert(
        TOPICS_EXCLUDE_CONFIG.to_string(),
        "test-exclude".to_string(),
    );

    let config = MirrorSourceConfig::new(props);
    let filter = config.topic_filter();

    // Included topics should match
    assert!(filter.should_replicate_topic("test-topic"));
    assert!(filter.should_replicate_topic("test123"));

    // Excluded topic should not match even if included pattern matches
    assert!(!filter.should_replicate_topic("test-exclude"));
}

/// Test: Internal topics are excluded by default.
#[test]
fn test_internal_topics_excluded_by_default() {
    let props = create_config_with_topics(".*");

    let config = MirrorSourceConfig::new(props);
    let filter = config.topic_filter();

    // Regular topics should be replicated
    assert!(filter.should_replicate_topic("my-topic"));

    // Internal topics should be excluded by default
    assert!(!filter.should_replicate_topic("__consumer_offsets"));
    assert!(!filter.should_replicate_topic("__transaction_state"));
    assert!(!filter.should_replicate_topic("__cluster_metadata"));

    // Topics ending with .internal should be excluded
    assert!(!filter.should_replicate_topic("test.internal"));
    assert!(!filter.should_replicate_topic("anything.internal"));
}

// ============================================================================
// testConfigPropertyMatching Tests - Config property filter behavior
// ============================================================================

/// Test: Config property filter excludes cluster-specific properties.
/// Corresponds to Java: MirrorSourceConfigTest.testConfigPropertyMatching()
#[test]
fn test_config_property_matching_excludes_cluster_specific() {
    let config = MirrorSourceConfig::default();
    let filter = config.config_property_filter();

    // Cluster-specific properties should be excluded
    assert!(!filter.should_replicate_config_property("min.insync.replicas"));
    assert!(!filter.should_replicate_config_property("follower.replication.throttled.replicas"));
    assert!(!filter.should_replicate_config_property("leader.replication.throttled.replicas"));
    assert!(!filter.should_replicate_config_property("message.timestamp.difference.max.ms"));
    assert!(!filter.should_replicate_config_property("message.timestamp.type"));
    assert!(!filter.should_replicate_config_property("unclean.leader.election.enable"));
}

/// Test: Config property filter includes general properties.
#[test]
fn test_config_property_matching_includes_general() {
    let config = MirrorSourceConfig::default();
    let filter = config.config_property_filter();

    // General properties should be included
    assert!(filter.should_replicate_config_property("cleanup.policy"));
    assert!(filter.should_replicate_config_property("retention.ms"));
    assert!(filter.should_replicate_config_property("compression.type"));
    assert!(filter.should_replicate_config_property("segment.bytes"));
    assert!(filter.should_replicate_config_property("max.message.bytes"));
}

/// Test: Custom config property exclude pattern.
#[test]
fn test_config_property_custom_exclude() {
    let props = create_config_with_config_exclude("custom\\.property,my.*");

    let config = MirrorSourceConfig::new(props);
    let filter = config.config_property_filter();

    // Custom excluded properties should not be replicated
    assert!(!filter.should_replicate_config_property("custom.property"));
    assert!(!filter.should_replicate_config_property("my.config"));

    // Other properties should be replicated
    assert!(filter.should_replicate_config_property("cleanup.policy"));
    assert!(filter.should_replicate_config_property("other.property"));
}

// ============================================================================
// Configuration Defaults Tests
// ============================================================================

/// Test: Default values for source and target cluster alias.
#[test]
fn test_cluster_alias_defaults() {
    let config = MirrorSourceConfig::default();

    assert_eq!(config.source_cluster_alias(), SOURCE_CLUSTER_ALIAS_DEFAULT);
    assert_eq!(config.target_cluster_alias(), TARGET_CLUSTER_ALIAS_DEFAULT);
}

/// Test: Default values for replication factor.
#[test]
fn test_replication_factor_defaults() {
    let config = MirrorSourceConfig::default();

    assert_eq!(config.replication_factor(), REPLICATION_FACTOR_DEFAULT);
    assert_eq!(
        config.offset_syncs_topic_replication_factor(),
        OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_DEFAULT
    );
}

/// Test: Custom replication factor values.
#[test]
fn test_replication_factor_custom() {
    let mut props = create_basic_config();
    props.insert(REPLICATION_FACTOR_CONFIG.to_string(), "5".to_string());
    props.insert(
        OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_CONFIG.to_string(),
        "2".to_string(),
    );

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.replication_factor(), 5);
    assert_eq!(config.offset_syncs_topic_replication_factor(), 2);
}

/// Test: Default consumer poll timeout.
#[test]
fn test_consumer_poll_timeout_default() {
    let config = MirrorSourceConfig::default();

    assert_eq!(
        config.consumer_poll_timeout(),
        Duration::from_millis(CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT as u64)
    );
}

/// Test: Custom consumer poll timeout.
#[test]
fn test_consumer_poll_timeout_custom() {
    let mut props = create_basic_config();
    props.insert(
        CONSUMER_POLL_TIMEOUT_MILLIS_CONFIG.to_string(),
        "5000".to_string(),
    );

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.consumer_poll_timeout(), Duration::from_millis(5000));
}

/// Test: Default offset lag max.
#[test]
fn test_offset_lag_max_default() {
    let config = MirrorSourceConfig::default();

    assert_eq!(config.max_offset_lag(), OFFSET_LAG_MAX_DEFAULT);
}

/// Test: Custom offset lag max.
#[test]
fn test_offset_lag_max_custom() {
    let mut props = create_basic_config();
    props.insert(OFFSET_LAG_MAX_CONFIG.to_string(), "500".to_string());

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.max_offset_lag(), 500);
}

// ============================================================================
// Interval Settings Tests
// ============================================================================

/// Test: Default refresh topics interval.
#[test]
fn test_refresh_topics_interval_default() {
    let config = MirrorSourceConfig::default();

    assert_eq!(
        config.refresh_topics_interval(),
        Duration::from_secs(REFRESH_TOPICS_INTERVAL_SECONDS_DEFAULT as u64)
    );
}

/// Test: Refresh topics disabled.
#[test]
fn test_refresh_topics_disabled() {
    let mut props = create_basic_config();
    props.insert(
        REFRESH_TOPICS_ENABLED_CONFIG.to_string(),
        "false".to_string(),
    );

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.refresh_topics_interval(), Duration::ZERO);
}

/// Test: Custom refresh topics interval.
#[test]
fn test_refresh_topics_interval_custom() {
    let mut props = create_basic_config();
    props.insert(
        REFRESH_TOPICS_INTERVAL_SECONDS_CONFIG.to_string(),
        "300".to_string(),
    );

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.refresh_topics_interval(), Duration::from_secs(300));
}

/// Test: Default sync topic configs interval.
#[test]
fn test_sync_topic_configs_interval_default() {
    let config = MirrorSourceConfig::default();

    assert_eq!(
        config.sync_topic_configs_interval(),
        Duration::from_secs(SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_DEFAULT as u64)
    );
}

/// Test: Sync topic configs disabled.
#[test]
fn test_sync_topic_configs_disabled() {
    let mut props = create_basic_config();
    props.insert(
        SYNC_TOPIC_CONFIGS_ENABLED_CONFIG.to_string(),
        "false".to_string(),
    );

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.sync_topic_configs_interval(), Duration::ZERO);
}

/// Test: Custom sync topic configs interval.
#[test]
fn test_sync_topic_configs_interval_custom() {
    let mut props = create_basic_config();
    props.insert(
        SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_CONFIG.to_string(),
        "120".to_string(),
    );

    let config = MirrorSourceConfig::new(props);

    assert_eq!(
        config.sync_topic_configs_interval(),
        Duration::from_secs(120)
    );
}

/// Test: Default sync topic ACLs interval.
#[test]
fn test_sync_topic_acls_interval_default() {
    let config = MirrorSourceConfig::default();

    assert_eq!(
        config.sync_topic_acls_interval(),
        Duration::from_secs(SYNC_TOPIC_ACLS_INTERVAL_SECONDS_DEFAULT as u64)
    );
}

/// Test: Sync topic ACLs disabled.
#[test]
fn test_sync_topic_acls_disabled() {
    let mut props = create_basic_config();
    props.insert(
        SYNC_TOPIC_ACLS_ENABLED_CONFIG.to_string(),
        "false".to_string(),
    );

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.sync_topic_acls_interval(), Duration::ZERO);
}

// ============================================================================
// Heartbeats Replication Tests
// ============================================================================

/// Test: Default heartbeats replication enabled.
#[test]
fn test_heartbeats_replication_enabled_default() {
    let config = MirrorSourceConfig::default();

    assert!(config.heartbeats_replication_enabled());
}

/// Test: Heartbeats replication disabled.
#[test]
fn test_heartbeats_replication_disabled() {
    let mut props = create_basic_config();
    props.insert(
        HEARTBEATS_REPLICATION_ENABLED_CONFIG.to_string(),
        "false".to_string(),
    );

    let config = MirrorSourceConfig::new(props);

    assert!(!config.heartbeats_replication_enabled());
}

// ============================================================================
// Emit Offset Syncs Tests
// ============================================================================

/// Test: Default emit offset syncs enabled.
#[test]
fn test_emit_offset_syncs_enabled_default() {
    let config = MirrorSourceConfig::default();

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

    let config = MirrorSourceConfig::new(props);

    assert!(!config.emit_offset_syncs_enabled());
}

// ============================================================================
// Topic Configuration Tests
// ============================================================================

/// Test: Default topics include pattern.
#[test]
fn test_topics_include_default() {
    let config = MirrorSourceConfig::default();

    assert_eq!(config.topics(), TOPICS_INCLUDE_DEFAULT);
}

/// Test: Default topics exclude pattern.
#[test]
fn test_topics_exclude_default() {
    let config = MirrorSourceConfig::default();

    assert_eq!(config.topics_exclude(), TOPICS_EXCLUDE_DEFAULT);
}

/// Test: Custom topics include pattern.
#[test]
fn test_topics_include_custom() {
    let props = create_config_with_topics("my-topics.*");

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.topics(), "my-topics.*");
}

/// Test: Custom topics exclude pattern.
#[test]
fn test_topics_exclude_custom() {
    let mut props = create_basic_config();
    props.insert(TOPICS_EXCLUDE_CONFIG.to_string(), "test.*".to_string());

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.topics_exclude(), "test.*");
}

// ============================================================================
// Config Properties Exclude Tests
// ============================================================================

/// Test: Default config properties exclude.
#[test]
fn test_config_properties_exclude_default() {
    let config = MirrorSourceConfig::default();

    assert_eq!(
        config.config_properties_exclude(),
        CONFIG_PROPERTIES_EXCLUDE_DEFAULT
    );
}

/// Test: Custom config properties exclude.
#[test]
fn test_config_properties_exclude_custom() {
    let props = create_config_with_config_exclude("custom\\.exclude,test.*");

    let config = MirrorSourceConfig::new(props);

    assert_eq!(
        config.config_properties_exclude(),
        "custom\\.exclude,test.*"
    );
}

// ============================================================================
// MirrorConnectorConfig Inherited Methods Tests
// ============================================================================

/// Test: Source cluster alias custom value.
#[test]
fn test_source_cluster_alias_custom() {
    let props = create_basic_config();

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.source_cluster_alias(), "backup");
}

/// Test: Target cluster alias custom value.
#[test]
fn test_target_cluster_alias_custom() {
    let props = create_basic_config();

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.target_cluster_alias(), "primary");
}

/// Test: Connector name.
#[test]
fn test_connector_name() {
    let props = create_basic_config();

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.connector_name(), Some("mirror-source-connector"));
}

/// Test: Connector name absent.
#[test]
fn test_connector_name_absent() {
    let mut props = HashMap::new();
    props.insert(
        SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
        "source".to_string(),
    );
    props.insert(
        TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
        "target".to_string(),
    );

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.connector_name(), None);
}

// ============================================================================
// TopicFilter Direct Tests
// ============================================================================

/// Test: DefaultTopicFilter with default patterns.
#[test]
fn test_default_topic_filter_default_patterns() {
    let filter = DefaultTopicFilter::new();

    // .* matches all topics
    assert!(filter.should_replicate_topic("any-topic"));

    // Internal topics are excluded
    assert!(!filter.should_replicate_topic("__consumer_offsets"));
    assert!(!filter.should_replicate_topic("__transaction_state"));
}

/// Test: DefaultTopicFilter with custom patterns.
#[test]
fn test_default_topic_filter_custom_patterns() {
    let mut filter = DefaultTopicFilter::new();
    let mut props = HashMap::new();
    props.insert(TOPICS_INCLUDE_CONFIG.to_string(), "prod.*".to_string());
    props.insert(TOPICS_EXCLUDE_CONFIG.to_string(), "prod-test".to_string());
    filter.configure(&props);

    assert!(filter.should_replicate_topic("prod-data"));
    assert!(!filter.should_replicate_topic("prod-test")); // excluded
    assert!(!filter.should_replicate_topic("dev-data")); // not matching include
}

// ============================================================================
// ConfigPropertyFilter Direct Tests
// ============================================================================

/// Test: DefaultConfigPropertyFilter with default patterns.
#[test]
fn test_default_config_property_filter_default_patterns() {
    let filter = DefaultConfigPropertyFilter::new();

    // Cluster-specific properties are excluded
    assert!(!filter.should_replicate_config_property("min.insync.replicas"));

    // General properties are included
    assert!(filter.should_replicate_config_property("cleanup.policy"));
}

/// Test: DefaultConfigPropertyFilter with custom patterns.
#[test]
fn test_default_config_property_filter_custom_patterns() {
    let mut filter = DefaultConfigPropertyFilter::new();
    let mut props = HashMap::new();
    props.insert(
        CONFIG_PROPERTIES_EXCLUDE_CONFIG.to_string(),
        "test.*".to_string(),
    );
    filter.configure(&props);

    assert!(!filter.should_replicate_config_property("test.property"));
    assert!(filter.should_replicate_config_property("cleanup.policy"));
}

// ============================================================================
// Edge Cases Tests
// ============================================================================

/// Test: Zero offset lag max.
#[test]
fn test_zero_offset_lag_max() {
    let mut props = create_basic_config();
    props.insert(OFFSET_LAG_MAX_CONFIG.to_string(), "0".to_string());

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.max_offset_lag(), 0);
}

/// Test: Very large replication factor.
#[test]
fn test_large_replication_factor() {
    let mut props = create_basic_config();
    props.insert(REPLICATION_FACTOR_CONFIG.to_string(), "100".to_string());

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.replication_factor(), 100);
}

/// Test: Topics config with extra whitespace.
#[test]
fn test_topics_with_whitespace() {
    let props = create_config_with_topics(" topic1 , topic2 , topic3 ");

    let config = MirrorSourceConfig::new(props);
    let filter = config.topic_filter();

    // After trimming, topics should match
    assert!(filter.should_replicate_topic("topic1"));
    assert!(filter.should_replicate_topic("topic2"));
    assert!(filter.should_replicate_topic("topic3"));
}

/// Test: Empty config creates valid defaults.
#[test]
fn test_empty_config_creates_defaults() {
    let config = MirrorSourceConfig::new(HashMap::new());

    // Should have all default values
    assert_eq!(config.source_cluster_alias(), SOURCE_CLUSTER_ALIAS_DEFAULT);
    assert_eq!(config.target_cluster_alias(), TARGET_CLUSTER_ALIAS_DEFAULT);
    assert_eq!(config.replication_factor(), REPLICATION_FACTOR_DEFAULT);
    assert!(config.emit_offset_syncs_enabled());
}

// ============================================================================
// Debug Format Tests
// ============================================================================

/// Test: MirrorSourceConfig Debug implementation.
#[test]
fn test_debug_format() {
    let config = MirrorSourceConfig::default();
    let debug_str = format!("{:?}", config);

    assert!(debug_str.contains("MirrorSourceConfig"));
    assert!(debug_str.contains("base_config"));
    assert!(debug_str.contains("topic_filter"));
    assert!(debug_str.contains("config_property_filter"));
}

// ============================================================================
// Originals and Get Tests
// ============================================================================

/// Test: originals returns the original config values.
#[test]
fn test_originals() {
    let props = create_basic_config();

    let config = MirrorSourceConfig::new(props.clone());
    let originals = config.originals();

    assert_eq!(
        originals.get(SOURCE_CLUSTER_ALIAS_CONFIG),
        Some(&"backup".to_string())
    );
    assert_eq!(
        originals.get(TARGET_CLUSTER_ALIAS_CONFIG),
        Some(&"primary".to_string())
    );
}

/// Test: get returns specific config value.
#[test]
fn test_get() {
    let mut props = create_basic_config();
    props.insert("custom.key".to_string(), "custom.value".to_string());

    let config = MirrorSourceConfig::new(props);

    assert_eq!(config.get("custom.key"), Some(&"custom.value".to_string()));
    assert_eq!(config.get("nonexistent.key"), None);
}
