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

//! MirrorSourceConfig - Configuration for MirrorSourceConnector.
//!
//! This module provides the MirrorSourceConfig class that defines configuration
//! options specific to the MirrorSourceConnector.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorSourceConfig

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use connect_mirror_client::ReplicationPolicy;

use crate::config::{
    MirrorConnectorConfig, EMIT_OFFSET_SYNCS_ENABLED_CONFIG, EMIT_OFFSET_SYNCS_ENABLED_DEFAULT,
    SOURCE_CLUSTER_ALIAS_DEFAULT, TARGET_CLUSTER_ALIAS_DEFAULT,
};

use super::config_property_filter::{
    ConfigPropertyFilter, DefaultConfigPropertyFilter, CONFIG_PROPERTIES_EXCLUDE_CONFIG,
    CONFIG_PROPERTIES_EXCLUDE_DEFAULT,
};
use super::topic_filter::{
    DefaultTopicFilter, TopicFilter, TOPICS_EXCLUDE_CONFIG, TOPICS_EXCLUDE_DEFAULT,
    TOPICS_INCLUDE_CONFIG, TOPICS_INCLUDE_DEFAULT,
};

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java MirrorSourceConfig
// ============================================================================

/// Configuration key for replication factor.
/// Corresponds to Java: MirrorSourceConfig.REPLICATION_FACTOR
pub const REPLICATION_FACTOR_CONFIG: &str = "replication.factor";

/// Default value for replication factor.
/// Corresponds to Java: MirrorSourceConfig.REPLICATION_FACTOR_DEFAULT
pub const REPLICATION_FACTOR_DEFAULT: i32 = 2;

/// Configuration key for offset syncs topic replication factor.
/// Corresponds to Java: MirrorSourceConfig.OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR
pub const OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_CONFIG: &str =
    "offset-syncs.topic.replication.factor";

/// Default value for offset syncs topic replication factor.
/// Corresponds to Java: MirrorSourceConfig.OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_DEFAULT
pub const OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_DEFAULT: i16 = 3;

/// Configuration key for consumer poll timeout.
/// Corresponds to Java: MirrorSourceConfig.CONSUMER_POLL_TIMEOUT_MILLIS
pub const CONSUMER_POLL_TIMEOUT_MILLIS_CONFIG: &str = "consumer.poll.timeout.ms";

/// Default value for consumer poll timeout.
/// Corresponds to Java: MirrorSourceConfig.CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT
pub const CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT: i64 = 1000;

/// Configuration key for refresh topics enabled.
/// Corresponds to Java: MirrorSourceConfig.REFRESH_TOPICS_ENABLED
pub const REFRESH_TOPICS_ENABLED_CONFIG: &str = "refresh.topics.enabled";

/// Default value for refresh topics enabled.
/// Corresponds to Java: MirrorSourceConfig.REFRESH_TOPICS_ENABLED_DEFAULT
pub const REFRESH_TOPICS_ENABLED_DEFAULT: bool = true;

/// Configuration key for refresh topics interval in seconds.
/// Corresponds to Java: MirrorSourceConfig.REFRESH_TOPICS_INTERVAL_SECONDS
pub const REFRESH_TOPICS_INTERVAL_SECONDS_CONFIG: &str = "refresh.topics.interval.seconds";

/// Default value for refresh topics interval in seconds.
/// Corresponds to Java: MirrorSourceConfig.REFRESH_TOPICS_INTERVAL_SECONDS_DEFAULT
pub const REFRESH_TOPICS_INTERVAL_SECONDS_DEFAULT: i64 = 600; // 10 minutes

/// Configuration key for sync topic configs enabled.
/// Corresponds to Java: MirrorSourceConfig.SYNC_TOPIC_CONFIGS_ENABLED
pub const SYNC_TOPIC_CONFIGS_ENABLED_CONFIG: &str = "sync.topic.configs.enabled";

/// Default value for sync topic configs enabled.
/// Corresponds to Java: MirrorSourceConfig.SYNC_TOPIC_CONFIGS_ENABLED_DEFAULT
pub const SYNC_TOPIC_CONFIGS_ENABLED_DEFAULT: bool = true;

/// Configuration key for sync topic configs interval in seconds.
/// Corresponds to Java: MirrorSourceConfig.SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS
pub const SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_CONFIG: &str = "sync.topic.configs.interval.seconds";

/// Default value for sync topic configs interval in seconds.
/// Corresponds to Java: MirrorSourceConfig.SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_DEFAULT
pub const SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_DEFAULT: i64 = 600; // 10 minutes

/// Configuration key for sync topic ACLs enabled.
/// Corresponds to Java: MirrorSourceConfig.SYNC_TOPIC_ACLS_ENABLED
pub const SYNC_TOPIC_ACLS_ENABLED_CONFIG: &str = "sync.topic.acls.enabled";

/// Default value for sync topic ACLs enabled.
/// Corresponds to Java: MirrorSourceConfig.SYNC_TOPIC_ACLS_ENABLED_DEFAULT
pub const SYNC_TOPIC_ACLS_ENABLED_DEFAULT: bool = true;

/// Configuration key for sync topic ACLs interval in seconds.
/// Corresponds to Java: MirrorSourceConfig.SYNC_TOPIC_ACLS_INTERVAL_SECONDS
pub const SYNC_TOPIC_ACLS_INTERVAL_SECONDS_CONFIG: &str = "sync.topic.acls.interval.seconds";

/// Default value for sync topic ACLs interval in seconds.
/// Corresponds to Java: MirrorSourceConfig.SYNC_TOPIC_ACLS_INTERVAL_SECONDS_DEFAULT
pub const SYNC_TOPIC_ACLS_INTERVAL_SECONDS_DEFAULT: i64 = 600; // 10 minutes

/// Configuration key for topic filter class.
/// Corresponds to Java: MirrorConnectorConfig.TOPIC_FILTER_CLASS
pub const TOPIC_FILTER_CLASS_CONFIG: &str = "topic.filter.class";

/// Configuration key for config property filter class.
/// Corresponds to Java: MirrorSourceConfig.CONFIG_PROPERTY_FILTER_CLASS
pub const CONFIG_PROPERTY_FILTER_CLASS_CONFIG: &str = "config.property.filter.class";

/// Configuration key for offset lag max.
/// Corresponds to Java: MirrorSourceConfig.OFFSET_LAG_MAX
pub const OFFSET_LAG_MAX_CONFIG: &str = "offset.lag.max";

/// Default value for offset lag max.
/// Corresponds to Java: MirrorSourceConfig.OFFSET_LAG_MAX_DEFAULT
pub const OFFSET_LAG_MAX_DEFAULT: i64 = 100;

/// Configuration key for heartbeats replication enabled.
/// Corresponds to Java: MirrorSourceConfig.HEARTBEATS_REPLICATION_ENABLED
pub const HEARTBEATS_REPLICATION_ENABLED_CONFIG: &str = "heartbeats.replication.enabled";

/// Default value for heartbeats replication enabled.
/// Corresponds to Java: MirrorSourceConfig.HEARTBEATS_REPLICATION_ENABLED_DEFAULT
pub const HEARTBEATS_REPLICATION_ENABLED_DEFAULT: bool = true;

/// Configuration key for offset syncs topic location.
/// Corresponds to Java: MirrorSourceConfig.OFFSET_SYNCS_TOPIC_LOCATION
pub const OFFSET_SYNCS_TOPIC_LOCATION_CONFIG: &str = "offset-syncs.topic.location";

/// Configuration key for task topic partitions (internal use).
/// Corresponds to Java: MirrorSourceConfig.TASK_TOPIC_PARTITIONS
pub const TASK_TOPIC_PARTITIONS_CONFIG: &str = "task.assigned.partitions";

/// Configuration key for task index (internal use).
/// Corresponds to Java: MirrorConnectorConfig.TASK_INDEX
pub const TASK_INDEX_CONFIG: &str = "task.index";

// ============================================================================
// MirrorSourceConfig
// ============================================================================

/// Configuration for MirrorSourceConnector.
///
/// This class provides configuration properties specific to the MirrorSourceConnector,
/// extending the base MirrorConnectorConfig with additional settings for topic
/// filtering, replication factor, and synchronization intervals.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.MirrorSourceConfig
pub struct MirrorSourceConfig {
    /// Base connector configuration
    base_config: MirrorConnectorConfig,
    /// Topic filter instance
    topic_filter: Arc<dyn TopicFilter>,
    /// Config property filter instance
    config_property_filter: Arc<dyn ConfigPropertyFilter>,
}

impl std::fmt::Debug for MirrorSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorSourceConfig")
            .field("base_config", &self.base_config)
            .field("topic_filter", &"Arc<dyn TopicFilter>")
            .field("config_property_filter", &"Arc<dyn ConfigPropertyFilter>")
            .finish()
    }
}

impl MirrorSourceConfig {
    /// Creates a new MirrorSourceConfig with the given configuration properties.
    ///
    /// Corresponds to Java: MirrorSourceConfig(Map<String, String> props)
    pub fn new(props: HashMap<String, String>) -> Self {
        let base_config = MirrorConnectorConfig::new(props.clone());

        // Create topic filter based on configuration
        let topic_filter = Self::create_topic_filter(&props);
        let config_property_filter = Self::create_config_property_filter(&props);

        MirrorSourceConfig {
            base_config,
            topic_filter,
            config_property_filter,
        }
    }

    /// Creates a topic filter based on the configuration.
    fn create_topic_filter(props: &HashMap<String, String>) -> Arc<dyn TopicFilter> {
        // Check if a custom topic filter class is specified
        let filter_class = props
            .get(TOPIC_FILTER_CLASS_CONFIG)
            .map(|s| s.as_str())
            .unwrap_or("");

        // For now, we only support DefaultTopicFilter
        // In the full implementation, this would dynamically load the class
        if filter_class.is_empty()
            || filter_class.contains("DefaultTopicFilter")
            || filter_class == "org.apache.kafka.connect.mirror.DefaultTopicFilter"
        {
            let mut filter = DefaultTopicFilter::new();
            filter.configure(props);
            Arc::new(filter)
        } else {
            // Default to DefaultTopicFilter for unknown class names
            let mut filter = DefaultTopicFilter::new();
            filter.configure(props);
            Arc::new(filter)
        }
    }

    /// Creates a config property filter based on the configuration.
    fn create_config_property_filter(
        props: &HashMap<String, String>,
    ) -> Arc<dyn ConfigPropertyFilter> {
        // Check if a custom config property filter class is specified
        let filter_class = props
            .get(CONFIG_PROPERTY_FILTER_CLASS_CONFIG)
            .map(|s| s.as_str())
            .unwrap_or("");

        // For now, we only support DefaultConfigPropertyFilter
        if filter_class.is_empty()
            || filter_class.contains("DefaultConfigPropertyFilter")
            || filter_class == "org.apache.kafka.connect.mirror.DefaultConfigPropertyFilter"
        {
            let mut filter = DefaultConfigPropertyFilter::new();
            filter.configure(props);
            Arc::new(filter)
        } else {
            // Default to DefaultConfigPropertyFilter for unknown class names
            let mut filter = DefaultConfigPropertyFilter::new();
            filter.configure(props);
            Arc::new(filter)
        }
    }

    /// Returns the topic filter for this configuration.
    ///
    /// The topic filter determines which topics should be replicated
    /// from the source cluster to the target cluster.
    ///
    /// Corresponds to Java: MirrorSourceConfig.topicFilter()
    pub fn topic_filter(&self) -> Arc<dyn TopicFilter> {
        self.topic_filter.clone()
    }

    /// Returns the config property filter for this configuration.
    ///
    /// The config property filter determines which topic configuration
    /// properties should be replicated.
    ///
    /// Corresponds to Java: MirrorSourceConfig.configPropertyFilter()
    pub fn config_property_filter(&self) -> Arc<dyn ConfigPropertyFilter> {
        self.config_property_filter.clone()
    }

    /// Returns the replication factor for newly created remote topics.
    ///
    /// Corresponds to Java: MirrorSourceConfig.replicationFactor()
    pub fn replication_factor(&self) -> i32 {
        self.base_config
            .get(REPLICATION_FACTOR_CONFIG)
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(REPLICATION_FACTOR_DEFAULT)
    }

    /// Returns the replication factor for the offset-syncs topic.
    ///
    /// Corresponds to Java: MirrorSourceConfig.offsetSyncsTopicReplicationFactor()
    pub fn offset_syncs_topic_replication_factor(&self) -> i16 {
        self.base_config
            .get(OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_CONFIG)
            .and_then(|s| s.parse::<i16>().ok())
            .unwrap_or(OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_DEFAULT)
    }

    /// Returns whether emit offset syncs is enabled.
    ///
    /// Corresponds to Java: MirrorSourceConfig.emitOffsetSyncsEnabled()
    pub fn emit_offset_syncs_enabled(&self) -> bool {
        self.base_config.emit_offset_syncs_enabled()
    }

    /// Returns the consumer poll timeout.
    ///
    /// Corresponds to Java: MirrorSourceConfig.consumerPollTimeout()
    pub fn consumer_poll_timeout(&self) -> Duration {
        let millis = self
            .base_config
            .get(CONSUMER_POLL_TIMEOUT_MILLIS_CONFIG)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(CONSUMER_POLL_TIMEOUT_MILLIS_DEFAULT as u64);
        Duration::from_millis(millis)
    }

    /// Returns the maximum offset lag.
    ///
    /// Corresponds to Java: MirrorSourceConfig.maxOffsetLag()
    pub fn max_offset_lag(&self) -> i64 {
        self.base_config
            .get(OFFSET_LAG_MAX_CONFIG)
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(OFFSET_LAG_MAX_DEFAULT)
    }

    /// Returns the interval for refreshing topics.
    ///
    /// Returns a negative duration if refresh is disabled.
    ///
    /// Corresponds to Java: MirrorSourceConfig.refreshTopicsInterval()
    pub fn refresh_topics_interval(&self) -> Duration {
        let enabled = self
            .base_config
            .get(REFRESH_TOPICS_ENABLED_CONFIG)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(REFRESH_TOPICS_ENABLED_DEFAULT);

        if enabled {
            let seconds = self
                .base_config
                .get(REFRESH_TOPICS_INTERVAL_SECONDS_CONFIG)
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(REFRESH_TOPICS_INTERVAL_SECONDS_DEFAULT as u64);
            Duration::from_secs(seconds)
        } else {
            Duration::ZERO // zero duration indicates disabled
        }
    }

    /// Returns the interval for syncing topic configurations.
    ///
    /// Returns Duration::ZERO if sync is disabled.
    ///
    /// Corresponds to Java: MirrorSourceConfig.syncTopicConfigsInterval()
    pub fn sync_topic_configs_interval(&self) -> Duration {
        let enabled = self
            .base_config
            .get(SYNC_TOPIC_CONFIGS_ENABLED_CONFIG)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(SYNC_TOPIC_CONFIGS_ENABLED_DEFAULT);

        if enabled {
            let seconds = self
                .base_config
                .get(SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_CONFIG)
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(SYNC_TOPIC_CONFIGS_INTERVAL_SECONDS_DEFAULT as u64);
            Duration::from_secs(seconds)
        } else {
            Duration::ZERO // zero duration indicates disabled
        }
    }

    /// Returns the interval for syncing topic ACLs.
    ///
    /// Returns Duration::ZERO if sync is disabled.
    ///
    /// Corresponds to Java: MirrorSourceConfig.syncTopicAclsInterval()
    pub fn sync_topic_acls_interval(&self) -> Duration {
        let enabled = self
            .base_config
            .get(SYNC_TOPIC_ACLS_ENABLED_CONFIG)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(SYNC_TOPIC_ACLS_ENABLED_DEFAULT);

        if enabled {
            let seconds = self
                .base_config
                .get(SYNC_TOPIC_ACLS_INTERVAL_SECONDS_CONFIG)
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(SYNC_TOPIC_ACLS_INTERVAL_SECONDS_DEFAULT as u64);
            Duration::from_secs(seconds)
        } else {
            Duration::ZERO // zero duration indicates disabled
        }
    }

    /// Returns whether heartbeats replication is enabled.
    ///
    /// Corresponds to Java: MirrorSourceConfig.heartbeatsReplicationEnabled()
    pub fn heartbeats_replication_enabled(&self) -> bool {
        self.base_config
            .get(HEARTBEATS_REPLICATION_ENABLED_CONFIG)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(HEARTBEATS_REPLICATION_ENABLED_DEFAULT)
    }

    /// Returns the topics to include (from topic filter configuration).
    ///
    /// Corresponds to Java: MirrorSourceConfig.TOPICS (via DefaultTopicFilter)
    pub fn topics(&self) -> &str {
        self.base_config
            .get(TOPICS_INCLUDE_CONFIG)
            .map(|s| s.as_str())
            .unwrap_or(TOPICS_INCLUDE_DEFAULT)
    }

    /// Returns the topics to exclude (from topic filter configuration).
    ///
    /// Corresponds to Java: MirrorSourceConfig.TOPICS_EXCLUDE (via DefaultTopicFilter)
    pub fn topics_exclude(&self) -> &str {
        self.base_config
            .get(TOPICS_EXCLUDE_CONFIG)
            .map(|s| s.as_str())
            .unwrap_or(TOPICS_EXCLUDE_DEFAULT)
    }

    /// Returns the config properties to exclude.
    ///
    /// Corresponds to Java: MirrorSourceConfig.CONFIG_PROPERTIES_EXCLUDE
    pub fn config_properties_exclude(&self) -> &str {
        self.base_config
            .get(CONFIG_PROPERTIES_EXCLUDE_CONFIG)
            .map(|s| s.as_str())
            .unwrap_or(CONFIG_PROPERTIES_EXCLUDE_DEFAULT)
    }

    /// Returns the offset syncs topic location.
    ///
    /// Corresponds to Java: MirrorSourceConfig.offsetSyncsTopicLocation()
    pub fn offset_syncs_topic_location(&self) -> &str {
        self.base_config
            .get(OFFSET_SYNCS_TOPIC_LOCATION_CONFIG)
            .map(|s| s.as_str())
            .unwrap_or(SOURCE_CLUSTER_ALIAS_DEFAULT)
    }

    // --- Inherited methods from MirrorConnectorConfig ---

    /// Returns the source cluster alias.
    ///
    /// Corresponds to Java: MirrorConnectorConfig.sourceClusterAlias()
    pub fn source_cluster_alias(&self) -> &str {
        self.base_config.source_cluster_alias()
    }

    /// Returns the target cluster alias.
    ///
    /// Corresponds to Java: MirrorConnectorConfig.targetClusterAlias()
    pub fn target_cluster_alias(&self) -> &str {
        self.base_config.target_cluster_alias()
    }

    /// Returns the replication policy.
    ///
    /// Corresponds to Java: MirrorConnectorConfig.replicationPolicy()
    pub fn replication_policy(&self) -> Arc<dyn ReplicationPolicy> {
        self.base_config.replication_policy()
    }

    /// Returns the connector name.
    ///
    /// Corresponds to Java: MirrorConnectorConfig.connectorName()
    pub fn connector_name(&self) -> Option<&str> {
        self.base_config.connector_name()
    }

    /// Returns the admin task timeout in milliseconds.
    ///
    /// Corresponds to Java: MirrorConnectorConfig.adminTaskTimeoutMillis()
    pub fn admin_task_timeout_millis(&self) -> i64 {
        self.base_config.admin_task_timeout_millis()
    }

    /// Returns the raw configuration value for the given key.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.base_config.get(key)
    }

    /// Returns all original configuration values.
    pub fn originals(&self) -> &HashMap<String, String> {
        self.base_config.originals()
    }

    /// Returns the offset syncs topic name.
    ///
    /// Corresponds to Java: MirrorSourceConfig.offsetSyncsTopic()
    pub fn offset_syncs_topic(&self) -> String {
        let other_cluster_alias =
            if self.offset_syncs_topic_location() == SOURCE_CLUSTER_ALIAS_DEFAULT {
                self.target_cluster_alias()
            } else {
                self.source_cluster_alias()
            };
        self.replication_policy()
            .offset_syncs_topic(other_cluster_alias)
    }

    /// Returns the checkpoints topic name.
    ///
    /// Corresponds to Java: MirrorSourceConfig.checkpointsTopic()
    pub fn checkpoints_topic(&self) -> String {
        self.replication_policy()
            .checkpoints_topic(self.source_cluster_alias())
    }
}

impl Default for MirrorSourceConfig {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_basic_config() -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert(
            crate::config::SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
            "backup".to_string(),
        );
        props.insert(
            crate::config::TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
            "primary".to_string(),
        );
        props.insert(
            crate::config::NAME_CONFIG.to_string(),
            "mirror-source-connector".to_string(),
        );
        props
    }

    #[test]
    fn test_source_cluster_alias() {
        let config = MirrorSourceConfig::new(create_basic_config());
        assert_eq!(config.source_cluster_alias(), "backup");
    }

    #[test]
    fn test_target_cluster_alias() {
        let config = MirrorSourceConfig::new(create_basic_config());
        assert_eq!(config.target_cluster_alias(), "primary");
    }

    #[test]
    fn test_connector_name() {
        let config = MirrorSourceConfig::new(create_basic_config());
        assert_eq!(config.connector_name(), Some("mirror-source-connector"));
    }

    #[test]
    fn test_replication_factor_default() {
        let config = MirrorSourceConfig::default();
        assert_eq!(config.replication_factor(), REPLICATION_FACTOR_DEFAULT);
    }

    #[test]
    fn test_replication_factor_custom() {
        let mut props = create_basic_config();
        props.insert(REPLICATION_FACTOR_CONFIG.to_string(), "3".to_string());
        let config = MirrorSourceConfig::new(props);
        assert_eq!(config.replication_factor(), 3);
    }

    #[test]
    fn test_emit_offset_syncs_enabled_default() {
        let config = MirrorSourceConfig::default();
        assert!(config.emit_offset_syncs_enabled());
    }

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

    #[test]
    fn test_topic_filter_default() {
        let config = MirrorSourceConfig::default();
        let filter = config.topic_filter();
        // Default filter should allow regular topics
        assert!(filter.should_replicate_topic("test-topic"));
    }

    #[test]
    fn test_config_property_filter_default() {
        let config = MirrorSourceConfig::default();
        let filter = config.config_property_filter();
        // Default filter should allow general properties
        assert!(filter.should_replicate_config_property("cleanup.policy"));
        // Default filter should exclude cluster-specific properties
        assert!(!filter.should_replicate_config_property("min.insync.replicas"));
    }

    #[test]
    fn test_consumer_poll_timeout_default() {
        let config = MirrorSourceConfig::default();
        assert_eq!(config.consumer_poll_timeout(), Duration::from_millis(1000));
    }

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

    #[test]
    fn test_refresh_topics_interval_enabled() {
        let config = MirrorSourceConfig::default();
        assert_eq!(
            config.refresh_topics_interval(),
            Duration::from_secs(600) // 10 minutes default
        );
    }

    #[test]
    fn test_refresh_topics_interval_disabled() {
        let mut props = create_basic_config();
        props.insert(
            REFRESH_TOPICS_ENABLED_CONFIG.to_string(),
            "false".to_string(),
        );
        let config = MirrorSourceConfig::new(props);
        assert_eq!(config.refresh_topics_interval(), Duration::ZERO);
    }

    #[test]
    fn test_max_offset_lag_default() {
        let config = MirrorSourceConfig::default();
        assert_eq!(config.max_offset_lag(), OFFSET_LAG_MAX_DEFAULT);
    }

    #[test]
    fn test_heartbeats_replication_enabled_default() {
        let config = MirrorSourceConfig::default();
        assert!(config.heartbeats_replication_enabled());
    }

    #[test]
    fn test_offset_syncs_topic_replication_factor_default() {
        let config = MirrorSourceConfig::default();
        assert_eq!(
            config.offset_syncs_topic_replication_factor(),
            OFFSET_SYNCS_TOPIC_REPLICATION_FACTOR_DEFAULT
        );
    }

    #[test]
    fn test_topics_default() {
        let config = MirrorSourceConfig::default();
        assert_eq!(config.topics(), TOPICS_INCLUDE_DEFAULT);
    }

    #[test]
    fn test_topics_exclude_default() {
        let config = MirrorSourceConfig::default();
        assert_eq!(config.topics_exclude(), TOPICS_EXCLUDE_DEFAULT);
    }
}
