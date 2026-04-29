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

//! MirrorHeartbeatConfig - Configuration for MirrorHeartbeatConnector.
//!
//! This module provides the MirrorHeartbeatConfig class that defines configuration
//! options specific to the MirrorHeartbeatConnector for emitting heartbeat records.
//!
//! The MirrorHeartbeatConnector emits heartbeat records to the target cluster to
//! monitor the health and connectivity of the replication process.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorHeartbeatConfig

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use connect_mirror_client::ReplicationPolicy;

use crate::config::MirrorConnectorConfig;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java MirrorHeartbeatConfig
// ============================================================================

/// Configuration key for emit heartbeats enabled.
/// Corresponds to Java: MirrorHeartbeatConfig.EMIT_HEARTBEATS_ENABLED
pub const EMIT_HEARTBEATS_ENABLED_CONFIG: &str = "emit.heartbeats.enabled";

/// Default value for emit heartbeats enabled.
/// Corresponds to Java: MirrorHeartbeatConfig.EMIT_HEARTBEATS_ENABLED_DEFAULT
pub const EMIT_HEARTBEATS_ENABLED_DEFAULT: bool = true;

/// Configuration key for emit heartbeats interval in seconds.
/// Corresponds to Java: MirrorHeartbeatConfig.EMIT_HEARTBEATS_INTERVAL_SECONDS
pub const EMIT_HEARTBEATS_INTERVAL_SECONDS_CONFIG: &str = "emit.heartbeats.interval.seconds";

/// Default value for emit heartbeats interval in seconds.
/// Corresponds to Java: MirrorHeartbeatConfig.EMIT_HEARTBEATS_INTERVAL_SECONDS_DEFAULT
pub const EMIT_HEARTBEATS_INTERVAL_SECONDS_DEFAULT: i64 = 1;

/// Configuration key for heartbeats topic replication factor.
/// Corresponds to Java: MirrorHeartbeatConfig.HEARTBEATS_TOPIC_REPLICATION_FACTOR
pub const HEARTBEATS_TOPIC_REPLICATION_FACTOR_CONFIG: &str = "heartbeats.topic.replication.factor";

/// Default value for heartbeats topic replication factor.
/// Corresponds to Java: MirrorHeartbeatConfig.HEARTBEATS_TOPIC_REPLICATION_FACTOR_DEFAULT
pub const HEARTBEATS_TOPIC_REPLICATION_FACTOR_DEFAULT: i16 = 3;

/// Task index configuration key.
/// Corresponds to Java: MirrorHeartbeatConfig.TASK_INDEX
pub const TASK_INDEX_CONFIG: &str = "task.index";

// ============================================================================
// MirrorHeartbeatConfig
// ============================================================================

/// Configuration for MirrorHeartbeatConnector.
///
/// This class provides configuration properties specific to the MirrorHeartbeatConnector,
/// extending the base MirrorConnectorConfig with additional settings for heartbeat emission.
///
/// The MirrorHeartbeatConnector emits heartbeat records to the target cluster to monitor
/// the health and connectivity of the replication process between source and target clusters.
/// Heartbeat records help determine reachability and latency between clusters.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.MirrorHeartbeatConfig
pub struct MirrorHeartbeatConfig {
    /// Base connector configuration
    base_config: MirrorConnectorConfig,
}

impl std::fmt::Debug for MirrorHeartbeatConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorHeartbeatConfig")
            .field("base_config", &self.base_config)
            .finish()
    }
}

impl MirrorHeartbeatConfig {
    /// Creates a new MirrorHeartbeatConfig with the given configuration properties.
    ///
    /// Corresponds to Java: MirrorHeartbeatConfig(Map<String, String> props)
    pub fn new(props: HashMap<String, String>) -> Self {
        let base_config = MirrorConnectorConfig::new(props);
        MirrorHeartbeatConfig { base_config }
    }

    /// Returns the interval for emitting heartbeats.
    ///
    /// Returns Duration::ZERO if heartbeat emission is disabled.
    ///
    /// Corresponds to Java: MirrorHeartbeatConfig.emitHeartbeatsInterval()
    pub fn emit_heartbeats_interval(&self) -> Duration {
        let enabled = self.emit_heartbeats_enabled();

        if enabled {
            let seconds = self
                .base_config
                .get(EMIT_HEARTBEATS_INTERVAL_SECONDS_CONFIG)
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(EMIT_HEARTBEATS_INTERVAL_SECONDS_DEFAULT as u64);
            Duration::from_secs(seconds)
        } else {
            Duration::ZERO
        }
    }

    /// Returns whether heartbeat emission is enabled.
    ///
    /// Corresponds to Java: MirrorHeartbeatConfig.emitHeartbeatsEnabled()
    pub fn emit_heartbeats_enabled(&self) -> bool {
        self.base_config
            .get(EMIT_HEARTBEATS_ENABLED_CONFIG)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(EMIT_HEARTBEATS_ENABLED_DEFAULT)
    }

    /// Returns the replication factor for the heartbeats topic.
    ///
    /// Corresponds to Java: MirrorHeartbeatConfig.heartbeatsTopicReplicationFactor()
    pub fn heartbeats_topic_replication_factor(&self) -> i16 {
        self.base_config
            .get(HEARTBEATS_TOPIC_REPLICATION_FACTOR_CONFIG)
            .and_then(|s| s.parse::<i16>().ok())
            .unwrap_or(HEARTBEATS_TOPIC_REPLICATION_FACTOR_DEFAULT)
    }

    /// Returns the heartbeats topic name.
    ///
    /// Corresponds to Java: MirrorHeartbeatConfig.heartbeatsTopic()
    /// Format: source_cluster_alias.heartbeats (for DefaultReplicationPolicy)
    pub fn heartbeats_topic(&self) -> String {
        // The ReplicationPolicy trait's heartbeats_topic() returns "heartbeats" by default
        // For MirrorMaker 2, we format it as: sourceAlias.heartbeats
        format!("{}.heartbeats", self.source_cluster_alias())
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

impl Default for MirrorHeartbeatConfig {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{NAME_CONFIG, SOURCE_CLUSTER_ALIAS_CONFIG, TARGET_CLUSTER_ALIAS_CONFIG};

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
            NAME_CONFIG.to_string(),
            "mirror-heartbeat-connector".to_string(),
        );
        props
    }

    #[test]
    fn test_source_cluster_alias() {
        let config = MirrorHeartbeatConfig::new(create_basic_config());
        assert_eq!(config.source_cluster_alias(), "backup");
    }

    #[test]
    fn test_target_cluster_alias() {
        let config = MirrorHeartbeatConfig::new(create_basic_config());
        assert_eq!(config.target_cluster_alias(), "primary");
    }

    #[test]
    fn test_connector_name() {
        let config = MirrorHeartbeatConfig::new(create_basic_config());
        assert_eq!(config.connector_name(), Some("mirror-heartbeat-connector"));
    }

    #[test]
    fn test_emit_heartbeats_interval_default() {
        let config = MirrorHeartbeatConfig::default();
        assert_eq!(config.emit_heartbeats_interval(), Duration::from_secs(1));
    }

    #[test]
    fn test_emit_heartbeats_interval_disabled() {
        let mut props = create_basic_config();
        props.insert(
            EMIT_HEARTBEATS_ENABLED_CONFIG.to_string(),
            "false".to_string(),
        );
        let config = MirrorHeartbeatConfig::new(props);
        assert_eq!(config.emit_heartbeats_interval(), Duration::ZERO);
    }

    #[test]
    fn test_emit_heartbeats_interval_custom() {
        let mut props = create_basic_config();
        props.insert(
            EMIT_HEARTBEATS_INTERVAL_SECONDS_CONFIG.to_string(),
            "5".to_string(),
        );
        let config = MirrorHeartbeatConfig::new(props);
        assert_eq!(config.emit_heartbeats_interval(), Duration::from_secs(5));
    }

    #[test]
    fn test_emit_heartbeats_enabled_default() {
        let config = MirrorHeartbeatConfig::default();
        assert!(config.emit_heartbeats_enabled());
    }

    #[test]
    fn test_emit_heartbeats_enabled_disabled() {
        let mut props = create_basic_config();
        props.insert(
            EMIT_HEARTBEATS_ENABLED_CONFIG.to_string(),
            "false".to_string(),
        );
        let config = MirrorHeartbeatConfig::new(props);
        assert!(!config.emit_heartbeats_enabled());
    }

    #[test]
    fn test_heartbeats_topic_replication_factor_default() {
        let config = MirrorHeartbeatConfig::default();
        assert_eq!(
            config.heartbeats_topic_replication_factor(),
            HEARTBEATS_TOPIC_REPLICATION_FACTOR_DEFAULT
        );
    }

    #[test]
    fn test_heartbeats_topic_replication_factor_custom() {
        let mut props = create_basic_config();
        props.insert(
            HEARTBEATS_TOPIC_REPLICATION_FACTOR_CONFIG.to_string(),
            "5".to_string(),
        );
        let config = MirrorHeartbeatConfig::new(props);
        assert_eq!(config.heartbeats_topic_replication_factor(), 5);
    }

    #[test]
    fn test_heartbeats_topic() {
        let config = MirrorHeartbeatConfig::new(create_basic_config());
        // Heartbeats topic should be formatted with source cluster alias
        // DefaultReplicationPolicy formats it as "clusterAlias.heartbeats"
        assert_eq!(config.heartbeats_topic(), "backup.heartbeats");
    }
}
