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

//! MirrorMakerConfig - Top-level config describing replication flows between multiple Kafka clusters.
//!
//! This module provides the MirrorMakerConfig class that manages cluster-level properties
//! and replication-level properties for MirrorMaker.
//!
//! Supports cluster-level properties of the form cluster.x.y.z, and replication-level
//! properties of the form source->target.x.y.z.
//!
//! Example configuration:
//! ```text
//! clusters = A, B, C
//! A.bootstrap.servers = aaa:9092
//! A.security.protocol = SSL
//! A->B.enabled = true
//! A->B.producer.client.id = "A-B-producer"
//! ```
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorMakerConfig

use std::collections::{HashMap, HashSet};

use connect_mirror_client::{MirrorClientConfig, SourceAndTarget};

use crate::checkpoint::MirrorCheckpointConfig;
use crate::config::{NAME_CONFIG, SOURCE_CLUSTER_ALIAS_CONFIG, TARGET_CLUSTER_ALIAS_CONFIG};
use crate::heartbeat::{
    MirrorHeartbeatConfig, EMIT_HEARTBEATS_ENABLED_CONFIG, EMIT_HEARTBEATS_ENABLED_DEFAULT,
};
use crate::source::MirrorSourceConfig;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java MirrorMakerConfig
// ============================================================================

/// Configuration key for clusters list.
/// Corresponds to Java: MirrorMakerConfig.CLUSTERS_CONFIG
pub const CLUSTERS_CONFIG: &str = "clusters";

/// Configuration key for config providers.
/// Corresponds to Java: MirrorMakerConfig.CONFIG_PROVIDERS_CONFIG
/// References WorkerConfig.CONFIG_PROVIDERS_CONFIG
pub const CONFIG_PROVIDERS_CONFIG: &str = "config.providers";

/// Configuration key for enabling internal REST server.
/// Corresponds to Java: MirrorMakerConfig.ENABLE_INTERNAL_REST_CONFIG
pub const ENABLE_INTERNAL_REST_CONFIG: &str = "dedicated.mode.enable.internal.rest";

/// Default value for enabling internal REST server.
/// Corresponds to Java: MirrorMakerConfig.ENABLE_INTERNAL_REST_DEFAULT (false)
pub const ENABLE_INTERNAL_REST_DEFAULT: bool = false;

// Internal constants (private in Java)
const CONNECTOR_CLASS: &str = "connector.class";
const GROUP_ID_CONFIG: &str = "group.id";
const KEY_CONVERTER_CLASS_CONFIG: &str = "key.converter";
const VALUE_CONVERTER_CLASS_CONFIG: &str = "value.converter";
const HEADER_CONVERTER_CLASS_CONFIG: &str = "header.converter";
const BYTE_ARRAY_CONVERTER_CLASS: &str = "org.apache.kafka.connect.converters.ByteArrayConverter";

const CLIENT_ID_CONFIG: &str = "client.id";
const OFFSET_STORAGE_TOPIC_CONFIG: &str = "offset.storage.topic";
const STATUS_STORAGE_TOPIC_CONFIG: &str = "status.storage.topic";
const CONFIG_TOPIC_CONFIG: &str = "config.storage.topic";

// Prefix constants
/// Prefix for source cluster configuration.
/// Corresponds to Java: MirrorMakerConfig.SOURCE_CLUSTER_PREFIX
pub const SOURCE_CLUSTER_PREFIX: &str = "source.cluster.";

/// Prefix for target cluster configuration.
/// Corresponds to Java: MirrorMakerConfig.TARGET_CLUSTER_PREFIX
pub const TARGET_CLUSTER_PREFIX: &str = "target.cluster.";

/// Prefix for source client configuration.
/// Corresponds to Java: MirrorMakerConfig.SOURCE_PREFIX
pub const SOURCE_PREFIX: &str = "source.";

/// Prefix for target client configuration.
/// Corresponds to Java: MirrorMakerConfig.TARGET_PREFIX
pub const TARGET_PREFIX: &str = "target.";

// ============================================================================
// MirrorMakerConfig
// ============================================================================

/// Top-level config describing replication flows between multiple Kafka clusters.
///
/// This class manages cluster-level properties (cluster.x.y.z) and replication-level
/// properties (source->target.x.y.z). It provides methods to extract configurations
/// for specific clusters and replication flows.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.MirrorMakerConfig
pub struct MirrorMakerConfig {
    /// Original configuration properties (after parsing)
    originals: HashMap<String, String>,
    /// Raw properties as provided by user
    raw_properties: HashMap<String, String>,
    /// Cached cluster names
    clusters: HashSet<String>,
}

impl std::fmt::Debug for MirrorMakerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorMakerConfig")
            .field("originals", &self.originals)
            .field("raw_properties", &self.raw_properties)
            .field("clusters", &self.clusters)
            .finish()
    }
}

impl MirrorMakerConfig {
    /// Creates a new MirrorMakerConfig with the given configuration properties.
    ///
    /// Corresponds to Java: MirrorMakerConfig(Map<String, String> props)
    pub fn new(props: HashMap<String, String>) -> Self {
        let originals = props.clone();
        let raw_properties = props.clone();
        let clusters = Self::parse_clusters(&props);

        MirrorMakerConfig {
            originals,
            raw_properties,
            clusters,
        }
    }

    /// Parses the clusters list from the configuration.
    ///
    /// The clusters configuration is a comma-separated list of cluster aliases.
    fn parse_clusters(props: &HashMap<String, String>) -> HashSet<String> {
        props
            .get(CLUSTERS_CONFIG)
            .map(|s| s.split(',').map(|c| c.trim().to_string()).collect())
            .unwrap_or_default()
    }

    /// Returns the set of cluster aliases defined in the configuration.
    ///
    /// Corresponds to Java: MirrorMakerConfig.clusters()
    pub fn clusters(&self) -> HashSet<String> {
        self.clusters.clone()
    }

    /// Returns whether the internal REST server is enabled.
    ///
    /// Corresponds to Java: MirrorMakerConfig.enableInternalRest()
    pub fn enable_internal_rest(&self) -> bool {
        self.originals
            .get(ENABLE_INTERNAL_REST_CONFIG)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(ENABLE_INTERNAL_REST_DEFAULT)
    }

    /// Returns the list of cluster pairs (source->target) that should be replicated.
    ///
    /// By default, all source->target combinations are created even if `x->y.enabled=false`,
    /// unless `emit.heartbeats.enabled=false` or `x->y.emit.heartbeats.enabled=false`.
    ///
    /// This behavior ensures that for a given replication flow A->B with heartbeats,
    /// 2 herders are required:
    /// - B->A for the MirrorHeartbeatConnector (emits heartbeats into A for monitoring)
    /// - A->B for the MirrorSourceConnector (actual replication flow)
    ///
    /// Corresponds to Java: MirrorMakerConfig.clusterPairs()
    pub fn cluster_pairs(&self) -> Vec<SourceAndTarget> {
        let mut pairs = Vec::new();
        let clusters = self.clusters();

        // Check global heartbeats enabled setting
        let global_heartbeats_enabled = self
            .originals
            .get(EMIT_HEARTBEATS_ENABLED_CONFIG)
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(EMIT_HEARTBEATS_ENABLED_DEFAULT);

        for source in &clusters {
            for target in &clusters {
                if source != target {
                    let cluster_pair_config_prefix = format!("{}->{}.", source, target);
                    let enabled_key = format!("{}enabled", cluster_pair_config_prefix);
                    let cluster_pair_enabled = self
                        .originals
                        .get(&enabled_key)
                        .and_then(|s| s.parse::<bool>().ok())
                        .unwrap_or(false);

                    // Check cluster pair specific heartbeats setting
                    let heartbeat_key = format!(
                        "{}{}",
                        cluster_pair_config_prefix, EMIT_HEARTBEATS_ENABLED_CONFIG
                    );
                    let cluster_pair_heartbeats_enabled = self
                        .originals
                        .get(&heartbeat_key)
                        .and_then(|s| s.parse::<bool>().ok())
                        .unwrap_or(global_heartbeats_enabled);

                    // Add pair if either enabled or heartbeats enabled
                    if cluster_pair_enabled || cluster_pair_heartbeats_enabled {
                        pairs.push(SourceAndTarget::new(source.clone(), target.clone()));
                    }
                }
            }
        }

        pairs
    }

    /// Constructs a MirrorClientConfig from properties of the form cluster.x.y.z.
    ///
    /// Use to connect to a cluster based on the MirrorMaker top-level config file.
    ///
    /// Corresponds to Java: MirrorMakerConfig.clientConfig(String cluster)
    pub fn client_config(&self, cluster: &str) -> MirrorClientConfig {
        let mut props = HashMap::new();
        props.extend(self.originals.clone());
        props.extend(self.cluster_props(cluster));
        MirrorClientConfig::new(props)
    }

    /// Loads properties of the form cluster.x.y.z.
    ///
    /// Corresponds to Java: MirrorMakerConfig.clusterProps(String cluster)
    fn cluster_props(&self, cluster: &str) -> HashMap<String, String> {
        let mut props = HashMap::new();

        // Load properties with cluster prefix stripped
        props.extend(self.strings_with_prefix_stripped(&format!("{}.", cluster)));

        // For MirrorClientConfig.CLIENT_CONFIG_DEF names, add producer/consumer/admin prefixed versions
        // The key client config properties that should be propagated
        let client_config_names = [
            "bootstrap.servers",
            "security.protocol",
            "sasl.mechanism",
            "sasl.jaas.config",
            "ssl.truststore.location",
            "ssl.truststore.password",
            "ssl.keystore.location",
            "ssl.keystore.password",
            "ssl.key.password",
            "client.id",
        ];

        for k in client_config_names {
            if let Some(v) = props.get(k).cloned() {
                props.insert_if_absent(format!("producer.{}", k), v.clone());
                props.insert_if_absent(format!("consumer.{}", k), v.clone());
                props.insert_if_absent(format!("admin.{}", k), v.clone());
            }
        }

        // Also include raw properties for these client config names
        for k in client_config_names {
            if let Some(v) = self.raw_properties.get(k).cloned() {
                props.insert_if_absent(format!("producer.{}", k), v.clone());
                props.insert_if_absent(format!("consumer.{}", k), v.clone());
                props.insert_if_absent(format!("admin.{}", k), v.clone());
                props.insert_if_absent(k.to_string(), v.clone());
            }
        }

        props
    }

    /// Loads worker configs based on properties of the form x.y.z and cluster.x.y.z.
    ///
    /// Corresponds to Java: MirrorMakerConfig.workerConfig(SourceAndTarget sourceAndTarget)
    pub fn worker_config(&self, source_and_target: &SourceAndTarget) -> HashMap<String, String> {
        let mut props = HashMap::new();

        // Add target cluster properties
        props.extend(self.cluster_props(source_and_target.target()));

        // Accept common top-level configs that are otherwise ignored by MM2
        props.extend(self.strings_with_prefix("offset.storage"));
        props.extend(self.strings_with_prefix("config.storage"));
        props.extend(self.strings_with_prefix("status.storage"));
        props.extend(self.strings_with_prefix("key.converter"));
        props.extend(self.strings_with_prefix("value.converter"));
        props.extend(self.strings_with_prefix("header.converter"));
        props.extend(self.strings_with_prefix("task"));
        props.extend(self.strings_with_prefix("worker"));
        props.extend(self.strings_with_prefix("replication.policy"));

        // Add config providers
        props.extend(self.strings_with_prefix(CONFIG_PROVIDERS_CONFIG));

        // Fill in reasonable defaults
        props.insert_if_absent(CLIENT_ID_CONFIG.to_string(), source_and_target.to_string());
        props.insert_if_absent(
            GROUP_ID_CONFIG.to_string(),
            format!("{}-mm2", source_and_target.source()),
        );
        props.insert_if_absent(
            OFFSET_STORAGE_TOPIC_CONFIG.to_string(),
            format!("mm2-offsets.{}.internal", source_and_target.source()),
        );
        props.insert_if_absent(
            STATUS_STORAGE_TOPIC_CONFIG.to_string(),
            format!("mm2-status.{}.internal", source_and_target.source()),
        );
        props.insert_if_absent(
            CONFIG_TOPIC_CONFIG.to_string(),
            format!("mm2-configs.{}.internal", source_and_target.source()),
        );
        props.insert_if_absent(
            KEY_CONVERTER_CLASS_CONFIG.to_string(),
            BYTE_ARRAY_CONVERTER_CLASS.to_string(),
        );
        props.insert_if_absent(
            VALUE_CONVERTER_CLASS_CONFIG.to_string(),
            BYTE_ARRAY_CONVERTER_CLASS.to_string(),
        );
        props.insert_if_absent(
            HEADER_CONVERTER_CLASS_CONFIG.to_string(),
            BYTE_ARRAY_CONVERTER_CLASS.to_string(),
        );

        props
    }

    /// Returns all configuration names from the connector config definitions.
    ///
    /// Corresponds to Java: MirrorMakerConfig.allConfigNames()
    fn all_config_names(&self) -> HashSet<String> {
        let mut all_names = HashSet::new();

        // Add MirrorCheckpointConfig config names
        all_names.extend(Self::checkpoint_config_names());
        // Add MirrorSourceConfig config names
        all_names.extend(Self::source_config_names());
        // Add MirrorHeartbeatConfig config names
        all_names.extend(Self::heartbeat_config_names());

        all_names
    }

    /// Returns config names from MirrorCheckpointConfig definition.
    fn checkpoint_config_names() -> HashSet<String> {
        [
            "emit.checkpoints.enabled",
            "emit.checkpoints.interval.seconds",
            "sync.group.offsets.enabled",
            "sync.group.offsets.interval.seconds",
            "checkpoints.topic.replication.factor",
            "task.assigned.groups",
            "topic.filter.class",
            "group.filter.class",
            "refresh.groups.enabled",
            "refresh.groups.interval.seconds",
            "sync.group.offsets.interval.seconds",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    /// Returns config names from MirrorSourceConfig definition.
    fn source_config_names() -> HashSet<String> {
        [
            "replication.factor",
            "offset-syncs.topic.replication.factor",
            "consumer.poll.timeout.ms",
            "refresh.topics.enabled",
            "refresh.topics.interval.seconds",
            "sync.topic.configs.enabled",
            "sync.topic.configs.interval.seconds",
            "sync.topic.acls.enabled",
            "sync.topic.acls.interval.seconds",
            "topic.filter.class",
            "config.property.filter.class",
            "offset.lag.max",
            "heartbeats.replication.enabled",
            "offset-syncs.topic.location",
            "task.assigned.partitions",
            "task.index",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    /// Returns config names from MirrorHeartbeatConfig definition.
    fn heartbeat_config_names() -> HashSet<String> {
        [
            "emit.heartbeats.enabled",
            "emit.heartbeats.interval.seconds",
            "heartbeats.topic.replication.factor",
            "task.index",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    /// Loads properties of the form cluster.x.y.z and source->target.x.y.z.
    ///
    /// Corresponds to Java: MirrorMakerConfig.connectorBaseConfig(SourceAndTarget sourceAndTarget, Class<?> connectorClass)
    pub fn connector_base_config(
        &self,
        source_and_target: &SourceAndTarget,
        connector_class: &str,
    ) -> HashMap<String, String> {
        let mut props = HashMap::new();

        // Start with raw properties, filtering to only include known config names
        let all_names = self.all_config_names();
        for (key, value) in &self.raw_properties {
            if all_names.contains(key.as_str()) {
                props.insert(key.clone(), value.clone());
            }
        }

        // Add config providers and replication policy
        props.extend(self.strings_with_prefix(CONFIG_PROVIDERS_CONFIG));
        props.extend(self.strings_with_prefix("replication.policy"));

        // Add source cluster properties
        let source_cluster_props = self.cluster_props(source_and_target.source());
        // Non prefixed with producer|consumer|admin -> source.cluster.xxx
        props.extend(Self::cluster_configs_with_prefix(
            SOURCE_CLUSTER_PREFIX,
            &source_cluster_props,
        ));
        // Prefixed with producer|consumer|admin -> source.xxx
        props.extend(Self::client_configs_with_prefix(
            SOURCE_PREFIX,
            &source_cluster_props,
        ));

        // Add target cluster properties
        let target_cluster_props = self.cluster_props(source_and_target.target());
        props.extend(Self::cluster_configs_with_prefix(
            TARGET_CLUSTER_PREFIX,
            &target_cluster_props,
        ));
        props.extend(Self::client_configs_with_prefix(
            TARGET_PREFIX,
            &target_cluster_props,
        ));

        // Set connector defaults
        // Extract simple class name from full class name
        let simple_name = connector_class
            .rsplit('.')
            .next()
            .unwrap_or(connector_class);
        props.insert_if_absent(NAME_CONFIG.to_string(), simple_name.to_string());
        props.insert_if_absent(CONNECTOR_CLASS.to_string(), connector_class.to_string());
        props.insert_if_absent(
            SOURCE_CLUSTER_ALIAS_CONFIG.to_string(),
            source_and_target.source().to_string(),
        );
        props.insert_if_absent(
            TARGET_CLUSTER_ALIAS_CONFIG.to_string(),
            source_and_target.target().to_string(),
        );

        // Override with connector-level properties (source->target.xxx)
        let pair_prefix = format!(
            "{}->{}.",
            source_and_target.source(),
            source_and_target.target()
        );
        props.extend(self.strings_with_prefix_stripped(&pair_prefix));

        // Disabled by default
        props.insert_if_absent(
            crate::config::ENABLED_CONFIG.to_string(),
            "false".to_string(),
        );

        props
    }

    /// Returns a MirrorSourceConfig for the given source and target.
    ///
    /// Corresponds to Java: MirrorMakerConfig.sourceConfig(SourceAndTarget sourceAndTarget)
    pub fn source_config(&self, source_and_target: &SourceAndTarget) -> MirrorSourceConfig {
        let props = self.connector_base_config(
            source_and_target,
            "org.apache.kafka.connect.mirror.MirrorSourceConnector",
        );
        MirrorSourceConfig::new(props)
    }

    /// Returns a MirrorCheckpointConfig for the given source and target.
    ///
    /// Corresponds to Java: MirrorMakerConfig.checkpointConfig(SourceAndTarget sourceAndTarget)
    pub fn checkpoint_config(&self, source_and_target: &SourceAndTarget) -> MirrorCheckpointConfig {
        let props = self.connector_base_config(
            source_and_target,
            "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
        );
        MirrorCheckpointConfig::new(props)
    }

    /// Returns a MirrorHeartbeatConfig for the given source and target.
    ///
    /// Corresponds to Java: MirrorMakerConfig.heartbeatConfig(SourceAndTarget sourceAndTarget)
    pub fn heartbeat_config(&self, source_and_target: &SourceAndTarget) -> MirrorHeartbeatConfig {
        let props = self.connector_base_config(
            source_and_target,
            "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
        );
        MirrorHeartbeatConfig::new(props)
    }

    /// Returns the list of config providers.
    ///
    /// Corresponds to Java: MirrorMakerConfig.configProviders()
    pub fn config_providers(&self) -> Vec<String> {
        self.originals
            .get(CONFIG_PROVIDERS_CONFIG)
            .map(|s| s.split(',').map(|p| p.trim().to_string()).collect())
            .unwrap_or_default()
    }

    // ============================================================================
    // Helper methods for prefix-based property extraction
    // ============================================================================

    /// Extracts properties with the given prefix, stripping the prefix from keys.
    ///
    /// Corresponds to Java: Utils.entriesWithPrefix(rawProperties, prefix)
    fn strings_with_prefix_stripped(&self, prefix: &str) -> HashMap<String, String> {
        self.raw_properties
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k[prefix.len()..].to_string(), v.clone()))
            .filter(|(k, v)| !k.is_empty() && !v.is_empty())
            .collect()
    }

    /// Extracts properties with the given prefix, keeping the prefix in keys.
    ///
    /// Corresponds to Java: Utils.entriesWithPrefix(rawProperties, prefix, false, true)
    fn strings_with_prefix(&self, prefix: &str) -> HashMap<String, String> {
        self.raw_properties
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Filters cluster configs (non-producer/consumer/admin) and adds prefix.
    ///
    /// Corresponds to Java: MirrorMakerConfig.clusterConfigsWithPrefix(String prefix, Map<String, String> props)
    fn cluster_configs_with_prefix(
        prefix: &str,
        props: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        props
            .iter()
            .filter(|(k, _)| {
                !k.starts_with("consumer") && !k.starts_with("producer") && !k.starts_with("admin")
            })
            .map(|(k, v)| (format!("{}{}", prefix, k), v.clone()))
            .collect()
    }

    /// Filters client configs (producer/consumer/admin) and adds prefix.
    ///
    /// Corresponds to Java: MirrorMakerConfig.clientConfigsWithPrefix(String prefix, Map<String, String> props)
    fn client_configs_with_prefix(
        prefix: &str,
        props: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        props
            .iter()
            .filter(|(k, _)| {
                k.starts_with("consumer") || k.starts_with("producer") || k.starts_with("admin")
            })
            .map(|(k, v)| (format!("{}{}", prefix, k), v.clone()))
            .collect()
    }

    /// Returns the raw configuration value for the given key.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.originals.get(key)
    }

    /// Returns all original configuration values.
    pub fn originals(&self) -> &HashMap<String, String> {
        &self.originals
    }

    /// Returns all raw configuration values.
    pub fn raw_properties(&self) -> &HashMap<String, String> {
        &self.raw_properties
    }
}

impl Default for MirrorMakerConfig {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

// Helper trait extension for HashMap
trait HashMapExt {
    fn insert_if_absent(&mut self, key: String, value: String);
}

impl HashMapExt for HashMap<String, String> {
    fn insert_if_absent(&mut self, key: String, value: String) {
        if !self.contains_key(&key) {
            self.insert(key, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_basic_config() -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert(CLUSTERS_CONFIG.to_string(), "A, B, C".to_string());
        props.insert("A.bootstrap.servers".to_string(), "aaa:9092".to_string());
        props.insert("B.bootstrap.servers".to_string(), "bbb:9092".to_string());
        props.insert("C.bootstrap.servers".to_string(), "ccc:9092".to_string());
        props.insert("A->B.enabled".to_string(), "true".to_string());
        props.insert("B->C.enabled".to_string(), "true".to_string());
        props
    }

    #[test]
    fn test_clusters() {
        let config = MirrorMakerConfig::new(create_basic_config());
        let clusters = config.clusters();
        assert!(clusters.contains("A"));
        assert!(clusters.contains("B"));
        assert!(clusters.contains("C"));
        assert_eq!(clusters.len(), 3);
    }

    #[test]
    fn test_clusters_empty() {
        let config = MirrorMakerConfig::default();
        let clusters = config.clusters();
        assert!(clusters.is_empty());
    }

    #[test]
    fn test_enable_internal_rest_default() {
        let config = MirrorMakerConfig::default();
        assert!(!config.enable_internal_rest());
    }

    #[test]
    fn test_enable_internal_rest_true() {
        let mut props = create_basic_config();
        props.insert(ENABLE_INTERNAL_REST_CONFIG.to_string(), "true".to_string());
        let config = MirrorMakerConfig::new(props);
        assert!(config.enable_internal_rest());
    }

    #[test]
    fn test_cluster_pairs() {
        let config = MirrorMakerConfig::new(create_basic_config());
        let pairs = config.cluster_pairs();
        // Should include A->B and B->C since they are enabled
        assert!(pairs.iter().any(|p| p.source() == "A" && p.target() == "B"));
        assert!(pairs.iter().any(|p| p.source() == "B" && p.target() == "C"));
    }

    #[test]
    fn test_cluster_pairs_with_heartbeats() {
        let mut props = create_basic_config();
        // Remove explicit enabled settings, rely on heartbeats
        props.remove("A->B.enabled");
        props.remove("B->C.enabled");
        // Set global heartbeats enabled
        props.insert(
            EMIT_HEARTBEATS_ENABLED_CONFIG.to_string(),
            "true".to_string(),
        );
        let config = MirrorMakerConfig::new(props);
        let pairs = config.cluster_pairs();
        // Should include many pairs since heartbeats are enabled globally
        // For 3 clusters: A->B, A->C, B->A, B->C, C->A, C->B (6 pairs)
        assert!(pairs.len() >= 6);
    }

    #[test]
    fn test_cluster_pairs_heartbeats_disabled() {
        let mut props = create_basic_config();
        props.remove("A->B.enabled");
        props.remove("B->C.enabled");
        props.insert(
            EMIT_HEARTBEATS_ENABLED_CONFIG.to_string(),
            "false".to_string(),
        );
        let config = MirrorMakerConfig::new(props);
        let pairs = config.cluster_pairs();
        // Should be empty since nothing is enabled
        assert!(pairs.is_empty());
    }

    #[test]
    fn test_client_config() {
        let config = MirrorMakerConfig::new(create_basic_config());
        let client_config = config.client_config("A");
        assert_eq!(
            client_config.get("bootstrap.servers"),
            Some(&"aaa:9092".to_string())
        );
    }

    #[test]
    fn test_worker_config() {
        let config = MirrorMakerConfig::new(create_basic_config());
        let source_and_target = SourceAndTarget::new("A", "B");
        let worker_config = config.worker_config(&source_and_target);

        // Check defaults are set
        assert!(worker_config.contains_key(CLIENT_ID_CONFIG));
        assert!(worker_config.contains_key(GROUP_ID_CONFIG));
        assert!(worker_config.contains_key(OFFSET_STORAGE_TOPIC_CONFIG));

        // Check group id format
        assert_eq!(
            worker_config.get(GROUP_ID_CONFIG),
            Some(&"A-mm2".to_string())
        );
    }

    #[test]
    fn test_connector_base_config() {
        let config = MirrorMakerConfig::new(create_basic_config());
        let source_and_target = SourceAndTarget::new("A", "B");
        let connector_config = config.connector_base_config(
            &source_and_target,
            "org.apache.kafka.connect.mirror.MirrorSourceConnector",
        );

        // Check required fields
        assert!(connector_config.contains_key(NAME_CONFIG));
        assert!(connector_config.contains_key(CONNECTOR_CLASS));
        assert!(connector_config.contains_key(SOURCE_CLUSTER_ALIAS_CONFIG));
        assert!(connector_config.contains_key(TARGET_CLUSTER_ALIAS_CONFIG));

        // Check values
        assert_eq!(
            connector_config.get(SOURCE_CLUSTER_ALIAS_CONFIG),
            Some(&"A".to_string())
        );
        assert_eq!(
            connector_config.get(TARGET_CLUSTER_ALIAS_CONFIG),
            Some(&"B".to_string())
        );
        assert_eq!(
            connector_config.get(CONNECTOR_CLASS),
            Some(&"org.apache.kafka.connect.mirror.MirrorSourceConnector".to_string())
        );
    }

    #[test]
    fn test_source_config() {
        let config = MirrorMakerConfig::new(create_basic_config());
        let source_and_target = SourceAndTarget::new("A", "B");
        let source_config = config.source_config(&source_and_target);

        assert_eq!(source_config.source_cluster_alias(), "A");
        assert_eq!(source_config.target_cluster_alias(), "B");
    }

    #[test]
    fn test_checkpoint_config() {
        let config = MirrorMakerConfig::new(create_basic_config());
        let source_and_target = SourceAndTarget::new("A", "B");
        let checkpoint_config = config.checkpoint_config(&source_and_target);

        assert_eq!(checkpoint_config.source_cluster_alias(), "A");
        assert_eq!(checkpoint_config.target_cluster_alias(), "B");
    }

    #[test]
    fn test_heartbeat_config() {
        let config = MirrorMakerConfig::new(create_basic_config());
        let source_and_target = SourceAndTarget::new("A", "B");
        let heartbeat_config = config.heartbeat_config(&source_and_target);

        assert_eq!(heartbeat_config.source_cluster_alias(), "A");
        assert_eq!(heartbeat_config.target_cluster_alias(), "B");
    }

    #[test]
    fn test_strings_with_prefix_stripped() {
        let mut props = HashMap::new();
        props.insert("A.bootstrap.servers".to_string(), "aaa:9092".to_string());
        props.insert("A.security.protocol".to_string(), "SSL".to_string());
        props.insert("B.bootstrap.servers".to_string(), "bbb:9092".to_string());
        let config = MirrorMakerConfig::new(props);

        let stripped = config.strings_with_prefix_stripped("A.");
        assert_eq!(
            stripped.get("bootstrap.servers"),
            Some(&"aaa:9092".to_string())
        );
        assert_eq!(stripped.get("security.protocol"), Some(&"SSL".to_string()));
        assert!(!stripped.contains_key("B.bootstrap.servers"));
    }

    #[test]
    fn test_strings_with_prefix() {
        let mut props = HashMap::new();
        props.insert("offset.storage.topic".to_string(), "offsets".to_string());
        props.insert("offset.storage.partitions".to_string(), "5".to_string());
        props.insert("config.storage.topic".to_string(), "configs".to_string());
        let config = MirrorMakerConfig::new(props);

        let prefixed = config.strings_with_prefix("offset.storage");
        assert_eq!(
            prefixed.get("offset.storage.topic"),
            Some(&"offsets".to_string())
        );
        assert_eq!(
            prefixed.get("offset.storage.partitions"),
            Some(&"5".to_string())
        );
        assert!(!prefixed.contains_key("config.storage.topic"));
    }

    #[test]
    fn test_config_providers() {
        let mut props = create_basic_config();
        props.insert(CONFIG_PROVIDERS_CONFIG.to_string(), "file, env".to_string());
        let config = MirrorMakerConfig::new(props);

        let providers = config.config_providers();
        assert_eq!(providers.len(), 2);
        assert!(providers.contains(&"file".to_string()));
        assert!(providers.contains(&"env".to_string()));
    }

    #[test]
    fn test_config_providers_empty() {
        let config = MirrorMakerConfig::default();
        let providers = config.config_providers();
        assert!(providers.is_empty());
    }
}
