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

//! MirrorConnectorConfig - Base configuration for MirrorMaker connectors.
//!
//! This module provides the MirrorConnectorConfig class that defines configuration
//! options specific to MirrorMaker connectors.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorConnectorConfig

use std::collections::HashMap;
use std::sync::Arc;

use connect_mirror_client::DefaultReplicationPolicy;
use connect_mirror_client::IdentityReplicationPolicy;
use connect_mirror_client::ReplicationPolicy;
use connect_mirror_client::REPLICATION_POLICY_CLASS;
use connect_mirror_client::REPLICATION_POLICY_CLASS_DEFAULT;
use connect_mirror_client::REPLICATION_POLICY_SEPARATOR;
use connect_mirror_client::REPLICATION_POLICY_SEPARATOR_DEFAULT;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java MirrorConnectorConfig
// ============================================================================

/// Configuration key for source cluster alias.
/// Corresponds to Java: MirrorConnectorConfig.SOURCE_CLUSTER_ALIAS
pub const SOURCE_CLUSTER_ALIAS_CONFIG: &str = "source.cluster.alias";

/// Default value for source cluster alias.
/// Corresponds to Java: MirrorConnectorConfig.SOURCE_CLUSTER_ALIAS_DEFAULT
pub const SOURCE_CLUSTER_ALIAS_DEFAULT: &str = "source";

/// Configuration key for target cluster alias.
/// Corresponds to Java: MirrorConnectorConfig.TARGET_CLUSTER_ALIAS
pub const TARGET_CLUSTER_ALIAS_CONFIG: &str = "target.cluster.alias";

/// Default value for target cluster alias.
/// Corresponds to Java: MirrorConnectorConfig.TARGET_CLUSTER_ALIAS_DEFAULT
pub const TARGET_CLUSTER_ALIAS_DEFAULT: &str = "target";

/// Configuration key for connector name.
/// Corresponds to Java: ConnectorConfig.NAME_CONFIG
pub const NAME_CONFIG: &str = "name";

/// Configuration key for admin task timeout in milliseconds.
/// Corresponds to Java: MirrorConnectorConfig.ADMIN_TASK_TIMEOUT_MILLIS
pub const ADMIN_TASK_TIMEOUT_MILLIS_CONFIG: &str = "admin.task.timeout.millis";

/// Default value for admin task timeout in milliseconds.
/// Corresponds to Java: MirrorConnectorConfig.ADMIN_TASK_TIMEOUT_MILLIS_DEFAULT
pub const ADMIN_TASK_TIMEOUT_MILLIS_DEFAULT: i64 = 60_000;

/// Configuration key for enabled flag.
/// Corresponds to Java: MirrorConnectorConfig.ENABLED
pub const ENABLED_CONFIG: &str = "enabled";

/// Configuration key for emit offset syncs enabled.
/// Corresponds to Java: MirrorConnectorConfig.EMIT_OFFSET_SYNCS_ENABLED
pub const EMIT_OFFSET_SYNCS_ENABLED_CONFIG: &str = "emit.offset.syncs.enabled";

/// Default value for emit offset syncs enabled.
/// Corresponds to Java: MirrorConnectorConfig.EMIT_OFFSET_SYNCS_ENABLED_DEFAULT
pub const EMIT_OFFSET_SYNCS_ENABLED_DEFAULT: bool = true;

// ============================================================================
// MirrorConnectorConfig
// ============================================================================

/// Base configuration for MirrorMaker connectors.
///
/// This class provides configuration properties shared by various MirrorMaker
/// connectors, including MirrorSourceConnector, MirrorCheckpointConnector,
/// and MirrorHeartbeatConnector.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.MirrorConnectorConfig
pub struct MirrorConnectorConfig {
    /// Original configuration properties
    originals: HashMap<String, String>,
    /// Cached replication policy instance
    replication_policy: Arc<dyn ReplicationPolicy>,
}

impl std::fmt::Debug for MirrorConnectorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorConnectorConfig")
            .field("originals", &self.originals)
            .field("replication_policy", &"Arc<dyn ReplicationPolicy>")
            .finish()
    }
}

impl MirrorConnectorConfig {
    /// Creates a new MirrorConnectorConfig with the given configuration properties.
    ///
    /// Corresponds to Java: MirrorConnectorConfig(ConfigDef configDef, Map<String, String> props)
    pub fn new(props: HashMap<String, String>) -> Self {
        let originals = props.clone();
        let replication_policy = Self::create_replication_policy(&props);
        MirrorConnectorConfig {
            originals,
            replication_policy,
        }
    }

    /// Creates a replication policy based on the configuration.
    ///
    /// The replication policy class is determined by the REPLICATION_POLICY_CLASS
    /// configuration property. If not specified, DefaultReplicationPolicy is used.
    fn create_replication_policy(props: &HashMap<String, String>) -> Arc<dyn ReplicationPolicy> {
        let class_name = props
            .get(REPLICATION_POLICY_CLASS)
            .map(|s| s.as_str())
            .unwrap_or(REPLICATION_POLICY_CLASS_DEFAULT);

        // Create the appropriate replication policy based on class name
        if class_name.contains("DefaultReplicationPolicy")
            || class_name == REPLICATION_POLICY_CLASS_DEFAULT
        {
            let mut policy = DefaultReplicationPolicy::new();
            policy.configure(props);
            Arc::new(policy)
        } else if class_name.contains("IdentityReplicationPolicy") {
            let mut policy = IdentityReplicationPolicy::new();
            policy.configure(props);
            Arc::new(policy)
        } else {
            // Default to DefaultReplicationPolicy for unknown class names
            let mut policy = DefaultReplicationPolicy::new();
            policy.configure(props);
            Arc::new(policy)
        }
    }

    /// Returns the source cluster alias.
    ///
    /// The source cluster alias identifies the cluster from which data is being replicated.
    ///
    /// Corresponds to Java: MirrorConnectorConfig.sourceClusterAlias()
    pub fn source_cluster_alias(&self) -> &str {
        self.originals
            .get(SOURCE_CLUSTER_ALIAS_CONFIG)
            .map(|s| s.as_str())
            .unwrap_or(SOURCE_CLUSTER_ALIAS_DEFAULT)
    }

    /// Returns the target cluster alias.
    ///
    /// The target cluster alias identifies the cluster to which data is being replicated.
    /// This alias is used in metrics reporting.
    ///
    /// Corresponds to Java: MirrorConnectorConfig.targetClusterAlias()
    pub fn target_cluster_alias(&self) -> &str {
        self.originals
            .get(TARGET_CLUSTER_ALIAS_CONFIG)
            .map(|s| s.as_str())
            .unwrap_or(TARGET_CLUSTER_ALIAS_DEFAULT)
    }

    /// Returns the replication policy.
    ///
    /// The replication policy defines the naming convention for remote topics.
    ///
    /// Corresponds to Java: MirrorConnectorConfig.replicationPolicy()
    pub fn replication_policy(&self) -> Arc<dyn ReplicationPolicy> {
        self.replication_policy.clone()
    }

    /// Returns the connector name.
    ///
    /// The connector name is retrieved from the NAME_CONFIG property.
    ///
    /// Corresponds to Java: MirrorConnectorConfig.connectorName()
    pub fn connector_name(&self) -> Option<&str> {
        self.originals.get(NAME_CONFIG).map(|s| s.as_str())
    }

    /// Returns the admin task timeout in milliseconds.
    ///
    /// Corresponds to Java: MirrorConnectorConfig.adminTaskTimeoutMillis()
    pub fn admin_task_timeout_millis(&self) -> i64 {
        self.originals
            .get(ADMIN_TASK_TIMEOUT_MILLIS_CONFIG)
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(ADMIN_TASK_TIMEOUT_MILLIS_DEFAULT)
    }

    /// Returns whether emit offset syncs is enabled.
    ///
    /// Corresponds to Java: MirrorConnectorConfig.emitOffsetSyncsEnabled()
    pub fn emit_offset_syncs_enabled(&self) -> bool {
        self.originals
            .get(EMIT_OFFSET_SYNCS_ENABLED_CONFIG)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(EMIT_OFFSET_SYNCS_ENABLED_DEFAULT)
    }

    /// Returns the replication policy separator.
    ///
    /// Corresponds to Java: MirrorConnectorConfig.replicationPolicySeparator()
    pub fn replication_policy_separator(&self) -> &str {
        self.originals
            .get(REPLICATION_POLICY_SEPARATOR)
            .map(|s| s.as_str())
            .unwrap_or(REPLICATION_POLICY_SEPARATOR_DEFAULT)
    }

    /// Returns the raw configuration value for the given key.
    ///
    /// Corresponds to Java: AbstractConfig.get(String key)
    pub fn get(&self, key: &str) -> Option<&String> {
        self.originals.get(key)
    }

    /// Returns all original configuration values.
    ///
    /// Corresponds to Java: AbstractConfig.originals()
    pub fn originals(&self) -> &HashMap<String, String> {
        &self.originals
    }

    /// Returns the string value for the given key, or default if not present.
    pub fn get_string(&self, key: &str, default: &str) -> String {
        self.originals
            .get(key)
            .cloned()
            .unwrap_or(default.to_string())
    }

    /// Returns the boolean value for the given key, or default if not present.
    pub fn get_bool(&self, key: &str, default: bool) -> bool {
        self.originals
            .get(key)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(default)
    }

    /// Returns the long value for the given key, or default if not present.
    pub fn get_long(&self, key: &str, default: i64) -> i64 {
        self.originals
            .get(key)
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(default)
    }
}

impl Default for MirrorConnectorConfig {
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
            SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
            "backup".to_string(),
        );
        props.insert(
            TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
            "primary".to_string(),
        );
        props.insert(
            NAME_CONFIG.to_string(),
            "mirror-source-connector".to_string(),
        );
        props
    }

    #[test]
    fn test_source_cluster_alias() {
        let config = MirrorConnectorConfig::new(create_basic_config());
        assert_eq!(config.source_cluster_alias(), "backup");
    }

    #[test]
    fn test_source_cluster_alias_default() {
        let config = MirrorConnectorConfig::default();
        assert_eq!(config.source_cluster_alias(), SOURCE_CLUSTER_ALIAS_DEFAULT);
    }

    #[test]
    fn test_target_cluster_alias() {
        let config = MirrorConnectorConfig::new(create_basic_config());
        assert_eq!(config.target_cluster_alias(), "primary");
    }

    #[test]
    fn test_target_cluster_alias_default() {
        let config = MirrorConnectorConfig::default();
        assert_eq!(config.target_cluster_alias(), TARGET_CLUSTER_ALIAS_DEFAULT);
    }

    #[test]
    fn test_connector_name() {
        let config = MirrorConnectorConfig::new(create_basic_config());
        assert_eq!(config.connector_name(), Some("mirror-source-connector"));
    }

    #[test]
    fn test_connector_name_none() {
        let config = MirrorConnectorConfig::default();
        assert_eq!(config.connector_name(), None);
    }

    #[test]
    fn test_replication_policy_default() {
        let config = MirrorConnectorConfig::default();
        let policy = config.replication_policy();
        // DefaultReplicationPolicy formats remote topic as "source.topic"
        let remote_topic = policy.format_remote_topic("backup", "test-topic");
        assert_eq!(remote_topic, "backup.test-topic");
    }

    #[test]
    fn test_replication_policy_identity() {
        let mut props = HashMap::new();
        props.insert(
            REPLICATION_POLICY_CLASS.to_string(),
            "org.apache.kafka.connect.mirror.IdentityReplicationPolicy".to_string(),
        );
        props.insert(
            SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
            "backup".to_string(),
        );
        let config = MirrorConnectorConfig::new(props);
        let policy = config.replication_policy();
        // IdentityReplicationPolicy does not rename topics (except heartbeats)
        let remote_topic = policy.format_remote_topic("backup", "test-topic");
        assert_eq!(remote_topic, "test-topic");
    }

    #[test]
    fn test_admin_task_timeout_default() {
        let config = MirrorConnectorConfig::default();
        assert_eq!(
            config.admin_task_timeout_millis(),
            ADMIN_TASK_TIMEOUT_MILLIS_DEFAULT
        );
    }

    #[test]
    fn test_emit_offset_syncs_default() {
        let config = MirrorConnectorConfig::default();
        assert!(config.emit_offset_syncs_enabled());
    }
}
