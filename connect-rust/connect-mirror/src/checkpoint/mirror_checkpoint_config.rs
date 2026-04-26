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

//! MirrorCheckpointConfig - Configuration for MirrorCheckpointConnector.
//!
//! This module provides the MirrorCheckpointConfig class that defines configuration
//! options specific to the MirrorCheckpointConnector for consumer group offset replication.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorCheckpointConfig

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use connect_mirror_client::ReplicationPolicy;

use crate::config::{
    MirrorConnectorConfig, SOURCE_CLUSTER_ALIAS_DEFAULT, TARGET_CLUSTER_ALIAS_DEFAULT,
};

use crate::source::topic_filter::{
    DefaultTopicFilter, TopicFilter, TOPICS_EXCLUDE_CONFIG, TOPICS_EXCLUDE_DEFAULT,
    TOPICS_INCLUDE_CONFIG, TOPICS_INCLUDE_DEFAULT,
};

use super::group_filter::{
    DefaultGroupFilter, GroupFilter, GROUPS_EXCLUDE_CONFIG, GROUPS_EXCLUDE_DEFAULT,
    GROUPS_INCLUDE_CONFIG, GROUPS_INCLUDE_DEFAULT,
};

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java MirrorCheckpointConfig
// ============================================================================

/// Configuration key for emit checkpoints enabled.
/// Corresponds to Java: MirrorCheckpointConfig.EMIT_CHECKPOINTS_ENABLED
pub const EMIT_CHECKPOINTS_ENABLED_CONFIG: &str = "emit.checkpoints.enabled";

/// Default value for emit checkpoints enabled.
/// Corresponds to Java: MirrorCheckpointConfig.EMIT_CHECKPOINTS_ENABLED_DEFAULT
pub const EMIT_CHECKPOINTS_ENABLED_DEFAULT: bool = true;

/// Configuration key for emit checkpoints interval in seconds.
/// Corresponds to Java: MirrorCheckpointConfig.EMIT_CHECKPOINTS_INTERVAL_SECONDS
pub const EMIT_CHECKPOINTS_INTERVAL_SECONDS_CONFIG: &str = "emit.checkpoints.interval.seconds";

/// Default value for emit checkpoints interval in seconds.
/// Corresponds to Java: MirrorCheckpointConfig.EMIT_CHECKPOINTS_INTERVAL_SECONDS_DEFAULT
pub const EMIT_CHECKPOINTS_INTERVAL_SECONDS_DEFAULT: i64 = 60; // 1 minute

/// Configuration key for sync group offsets enabled.
/// Corresponds to Java: MirrorCheckpointConfig.SYNC_GROUP_OFFSETS_ENABLED
pub const SYNC_GROUP_OFFSETS_ENABLED_CONFIG: &str = "sync.group.offsets.enabled";

/// Default value for sync group offsets enabled.
/// Corresponds to Java: MirrorCheckpointConfig.SYNC_GROUP_OFFSETS_ENABLED_DEFAULT
pub const SYNC_GROUP_OFFSETS_ENABLED_DEFAULT: bool = false;

/// Configuration key for sync group offsets interval in seconds.
/// Corresponds to Java: MirrorCheckpointConfig.SYNC_GROUP_OFFSETS_INTERVAL_SECONDS
pub const SYNC_GROUP_OFFSETS_INTERVAL_SECONDS_CONFIG: &str = "sync.group.offsets.interval.seconds";

/// Default value for sync group offsets interval in seconds.
/// Corresponds to Java: MirrorCheckpointConfig.SYNC_GROUP_OFFSETS_INTERVAL_SECONDS_DEFAULT
pub const SYNC_GROUP_OFFSETS_INTERVAL_SECONDS_DEFAULT: i64 = 600; // 10 minutes

/// Configuration key for checkpoints topic replication factor.
/// Corresponds to Java: MirrorCheckpointConfig.CHECKPOINTS_TOPIC_REPLICATION_FACTOR
pub const CHECKPOINTS_TOPIC_REPLICATION_FACTOR_CONFIG: &str =
    "checkpoints.topic.replication.factor";

/// Default value for checkpoints topic replication factor.
/// Corresponds to Java: MirrorCheckpointConfig.CHECKPOINTS_TOPIC_REPLICATION_FACTOR_DEFAULT
pub const CHECKPOINTS_TOPIC_REPLICATION_FACTOR_DEFAULT: i16 = 3;

/// Configuration key for task assigned groups (internal use).
/// Corresponds to Java: MirrorCheckpointConfig.TASK_ASSIGNED_GROUPS
pub const TASK_ASSIGNED_GROUPS_CONFIG: &str = "task.assigned.groups";

/// Configuration key for topic filter class.
/// Corresponds to Java: MirrorCheckpointConfig.TOPIC_FILTER_CLASS
pub const TOPIC_FILTER_CLASS_CONFIG: &str = "topic.filter.class";

/// Configuration key for group filter class.
/// Corresponds to Java: MirrorCheckpointConfig.GROUP_FILTER_CLASS
pub const GROUP_FILTER_CLASS_CONFIG: &str = "group.filter.class";

// ============================================================================
// MirrorCheckpointConfig
// ============================================================================

/// Configuration for MirrorCheckpointConnector.
///
/// This class provides configuration properties specific to the MirrorCheckpointConnector,
/// extending the base MirrorConnectorConfig with additional settings for consumer group
/// filtering, checkpoint emission, and offset synchronization.
///
/// The MirrorCheckpointConnector replicates consumer group offsets from the source cluster
/// to the target cluster, enabling consumers on the target cluster to resume from the
/// same position as consumers on the source cluster.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.MirrorCheckpointConfig
pub struct MirrorCheckpointConfig {
    /// Base connector configuration
    base_config: MirrorConnectorConfig,
    /// Topic filter instance
    topic_filter: Arc<dyn TopicFilter>,
    /// Group filter instance
    group_filter: Arc<dyn GroupFilter>,
}

impl std::fmt::Debug for MirrorCheckpointConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorCheckpointConfig")
            .field("base_config", &self.base_config)
            .field("topic_filter", &"Arc<dyn TopicFilter>")
            .field("group_filter", &"Arc<dyn GroupFilter>")
            .finish()
    }
}

impl MirrorCheckpointConfig {
    /// Creates a new MirrorCheckpointConfig with the given configuration properties.
    ///
    /// Corresponds to Java: MirrorCheckpointConfig(Map<String, String> props)
    pub fn new(props: HashMap<String, String>) -> Self {
        let base_config = MirrorConnectorConfig::new(props.clone());

        // Create topic filter based on configuration
        let topic_filter = Self::create_topic_filter(&props);
        let group_filter = Self::create_group_filter(&props);

        MirrorCheckpointConfig {
            base_config,
            topic_filter,
            group_filter,
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

    /// Creates a group filter based on the configuration.
    fn create_group_filter(props: &HashMap<String, String>) -> Arc<dyn GroupFilter> {
        // Check if a custom group filter class is specified
        let filter_class = props
            .get(GROUP_FILTER_CLASS_CONFIG)
            .map(|s| s.as_str())
            .unwrap_or("");

        // For now, we only support DefaultGroupFilter
        if filter_class.is_empty()
            || filter_class.contains("DefaultGroupFilter")
            || filter_class == "org.apache.kafka.connect.mirror.DefaultGroupFilter"
        {
            let mut filter = DefaultGroupFilter::new();
            filter.configure(props);
            Arc::new(filter)
        } else {
            // Default to DefaultGroupFilter for unknown class names
            let mut filter = DefaultGroupFilter::new();
            filter.configure(props);
            Arc::new(filter)
        }
    }

    /// Returns the topic filter for this configuration.
    ///
    /// The topic filter determines which topics are relevant for consumer group
    /// offset replication. Only consumer groups that are consuming topics matching
    /// the filter will have their checkpoints replicated.
    ///
    /// Corresponds to Java: MirrorCheckpointConfig.topicFilter()
    pub fn topic_filter(&self) -> Arc<dyn TopicFilter> {
        self.topic_filter.clone()
    }

    /// Returns the group filter for this configuration.
    ///
    /// The group filter determines which consumer groups should have their
    /// offsets replicated from the source cluster to the target cluster.
    ///
    /// Corresponds to Java: MirrorCheckpointConfig.groupFilter()
    pub fn group_filter(&self) -> Arc<dyn GroupFilter> {
        self.group_filter.clone()
    }

    /// Returns the interval for emitting checkpoints.
    ///
    /// Returns Duration::ZERO if checkpoint emission is disabled.
    ///
    /// Corresponds to Java: MirrorCheckpointConfig.emitCheckpointsInterval()
    pub fn emit_checkpoints_interval(&self) -> Duration {
        let enabled = self
            .base_config
            .get(EMIT_CHECKPOINTS_ENABLED_CONFIG)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(EMIT_CHECKPOINTS_ENABLED_DEFAULT);

        if enabled {
            let seconds = self
                .base_config
                .get(EMIT_CHECKPOINTS_INTERVAL_SECONDS_CONFIG)
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(EMIT_CHECKPOINTS_INTERVAL_SECONDS_DEFAULT as u64);
            Duration::from_secs(seconds)
        } else {
            Duration::ZERO
        }
    }

    /// Returns whether checkpoint emission is enabled.
    ///
    /// Corresponds to Java: MirrorCheckpointConfig.emitCheckpointsEnabled()
    pub fn emit_checkpoints_enabled(&self) -> bool {
        self.base_config
            .get(EMIT_CHECKPOINTS_ENABLED_CONFIG)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(EMIT_CHECKPOINTS_ENABLED_DEFAULT)
    }

    /// Returns the interval for syncing group offsets.
    ///
    /// Returns Duration::ZERO if sync is disabled.
    ///
    /// Corresponds to Java: MirrorCheckpointConfig.syncGroupOffsetsInterval()
    pub fn sync_group_offsets_interval(&self) -> Duration {
        let enabled = self
            .base_config
            .get(SYNC_GROUP_OFFSETS_ENABLED_CONFIG)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(SYNC_GROUP_OFFSETS_ENABLED_DEFAULT);

        if enabled {
            let seconds = self
                .base_config
                .get(SYNC_GROUP_OFFSETS_INTERVAL_SECONDS_CONFIG)
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(SYNC_GROUP_OFFSETS_INTERVAL_SECONDS_DEFAULT as u64);
            Duration::from_secs(seconds)
        } else {
            Duration::ZERO
        }
    }

    /// Returns whether syncing group offsets is enabled.
    ///
    /// Corresponds to Java: MirrorCheckpointConfig.syncGroupOffsetsEnabled()
    pub fn sync_group_offsets_enabled(&self) -> bool {
        self.base_config
            .get(SYNC_GROUP_OFFSETS_ENABLED_CONFIG)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(SYNC_GROUP_OFFSETS_ENABLED_DEFAULT)
    }

    /// Returns the replication factor for the checkpoints topic.
    ///
    /// Corresponds to Java: MirrorCheckpointConfig.checkpointsTopicReplicationFactor()
    pub fn checkpoints_topic_replication_factor(&self) -> i16 {
        self.base_config
            .get(CHECKPOINTS_TOPIC_REPLICATION_FACTOR_CONFIG)
            .and_then(|s| s.parse::<i16>().ok())
            .unwrap_or(CHECKPOINTS_TOPIC_REPLICATION_FACTOR_DEFAULT)
    }

    /// Returns the consumer groups to include (from group filter configuration).
    ///
    /// Corresponds to Java: MirrorCheckpointConfig.GROUPS (via DefaultGroupFilter)
    pub fn groups(&self) -> &str {
        self.base_config
            .get(GROUPS_INCLUDE_CONFIG)
            .map(|s| s.as_str())
            .unwrap_or(GROUPS_INCLUDE_DEFAULT)
    }

    /// Returns the consumer groups to exclude (from group filter configuration).
    ///
    /// Corresponds to Java: MirrorCheckpointConfig.GROUPS_EXCLUDE (via DefaultGroupFilter)
    pub fn groups_exclude(&self) -> &str {
        self.base_config
            .get(GROUPS_EXCLUDE_CONFIG)
            .map(|s| s.as_str())
            .unwrap_or(GROUPS_EXCLUDE_DEFAULT)
    }

    /// Returns the topics to include (from topic filter configuration).
    ///
    /// Corresponds to Java: MirrorCheckpointConfig.TOPICS (via DefaultTopicFilter)
    pub fn topics(&self) -> &str {
        self.base_config
            .get(TOPICS_INCLUDE_CONFIG)
            .map(|s| s.as_str())
            .unwrap_or(TOPICS_INCLUDE_DEFAULT)
    }

    /// Returns the topics to exclude (from topic filter configuration).
    ///
    /// Corresponds to Java: MirrorCheckpointConfig.TOPICS_EXCLUDE (via DefaultTopicFilter)
    pub fn topics_exclude(&self) -> &str {
        self.base_config
            .get(TOPICS_EXCLUDE_CONFIG)
            .map(|s| s.as_str())
            .unwrap_or(TOPICS_EXCLUDE_DEFAULT)
    }

    /// Returns the checkpoints topic name.
    ///
    /// Corresponds to Java: MirrorCheckpointConfig.checkpointsTopic()
    pub fn checkpoints_topic(&self) -> String {
        self.replication_policy()
            .checkpoints_topic(self.source_cluster_alias())
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
}

impl Default for MirrorCheckpointConfig {
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
            "mirror-checkpoint-connector".to_string(),
        );
        props
    }

    #[test]
    fn test_source_cluster_alias() {
        let config = MirrorCheckpointConfig::new(create_basic_config());
        assert_eq!(config.source_cluster_alias(), "backup");
    }

    #[test]
    fn test_target_cluster_alias() {
        let config = MirrorCheckpointConfig::new(create_basic_config());
        assert_eq!(config.target_cluster_alias(), "primary");
    }

    #[test]
    fn test_connector_name() {
        let config = MirrorCheckpointConfig::new(create_basic_config());
        assert_eq!(config.connector_name(), Some("mirror-checkpoint-connector"));
    }

    #[test]
    fn test_emit_checkpoints_interval_default() {
        let config = MirrorCheckpointConfig::default();
        assert_eq!(config.emit_checkpoints_interval(), Duration::from_secs(60));
    }

    #[test]
    fn test_emit_checkpoints_interval_disabled() {
        let mut props = create_basic_config();
        props.insert(
            EMIT_CHECKPOINTS_ENABLED_CONFIG.to_string(),
            "false".to_string(),
        );
        let config = MirrorCheckpointConfig::new(props);
        assert_eq!(config.emit_checkpoints_interval(), Duration::ZERO);
    }

    #[test]
    fn test_emit_checkpoints_interval_custom() {
        let mut props = create_basic_config();
        props.insert(
            EMIT_CHECKPOINTS_INTERVAL_SECONDS_CONFIG.to_string(),
            "300".to_string(),
        );
        let config = MirrorCheckpointConfig::new(props);
        assert_eq!(config.emit_checkpoints_interval(), Duration::from_secs(300));
    }

    #[test]
    fn test_sync_group_offsets_interval_default() {
        let config = MirrorCheckpointConfig::default();
        // Default is disabled
        assert_eq!(config.sync_group_offsets_interval(), Duration::ZERO);
    }

    #[test]
    fn test_sync_group_offsets_interval_enabled() {
        let mut props = create_basic_config();
        props.insert(
            SYNC_GROUP_OFFSETS_ENABLED_CONFIG.to_string(),
            "true".to_string(),
        );
        let config = MirrorCheckpointConfig::new(props);
        assert_eq!(
            config.sync_group_offsets_interval(),
            Duration::from_secs(600)
        );
    }

    #[test]
    fn test_checkpoints_topic_replication_factor_default() {
        let config = MirrorCheckpointConfig::default();
        assert_eq!(
            config.checkpoints_topic_replication_factor(),
            CHECKPOINTS_TOPIC_REPLICATION_FACTOR_DEFAULT
        );
    }

    #[test]
    fn test_checkpoints_topic_replication_factor_custom() {
        let mut props = create_basic_config();
        props.insert(
            CHECKPOINTS_TOPIC_REPLICATION_FACTOR_CONFIG.to_string(),
            "5".to_string(),
        );
        let config = MirrorCheckpointConfig::new(props);
        assert_eq!(config.checkpoints_topic_replication_factor(), 5);
    }

    #[test]
    fn test_topic_filter_default() {
        let config = MirrorCheckpointConfig::default();
        let filter = config.topic_filter();
        // Default filter should allow regular topics
        assert!(filter.should_replicate_topic("test-topic"));
    }

    #[test]
    fn test_group_filter_default() {
        let config = MirrorCheckpointConfig::default();
        let filter = config.group_filter();
        // Default filter should allow regular groups
        assert!(filter.should_replicate_group("my-consumer-group"));
        // Default filter should exclude console consumer groups
        assert!(!filter.should_replicate_group("console-consumer-12345"));
    }

    #[test]
    fn test_groups_default() {
        let config = MirrorCheckpointConfig::default();
        assert_eq!(config.groups(), GROUPS_INCLUDE_DEFAULT);
    }

    #[test]
    fn test_groups_exclude_default() {
        let config = MirrorCheckpointConfig::default();
        assert_eq!(config.groups_exclude(), GROUPS_EXCLUDE_DEFAULT);
    }

    #[test]
    fn test_checkpoints_topic() {
        let config = MirrorCheckpointConfig::new(create_basic_config());
        // Checkpoints topic should be formatted with source cluster alias
        // DefaultReplicationPolicy formats it as "clusterAlias.checkpoints.internal"
        assert_eq!(config.checkpoints_topic(), "backup.checkpoints.internal");
    }
}
