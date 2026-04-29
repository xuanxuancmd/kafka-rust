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

//! MirrorHerder - Herder extension for MirrorMaker that automatically configures connectors.
//!
//! MirrorHerder extends the DistributedHerder functionality by automatically configuring
//! MirrorMaker connectors (MirrorSourceConnector, MirrorHeartbeatConnector, MirrorCheckpointConnector)
//! when the worker becomes the leader.
//!
//! This ensures that replication flows are automatically established when a MirrorMaker
//! instance joins the cluster and becomes the leader.
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorHerder (~92 lines)

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use common_trait::herder::{
    ActiveTopicsInfo, Callback, ConfigInfos, ConfigKeyInfo, ConnectorInfo, ConnectorOffsets,
    ConnectorState, ConnectorStateInfo, ConnectorTaskId, Created, Herder, HerderRequest,
    InternalRequestSignature, LoggerLevel, Message, PluginDesc, Plugins, RestartRequest,
    StatusBackingStore, TargetState, TaskInfo, TaskStateInfo, VersionRange,
};
use connect_mirror_client::SourceAndTarget;

use crate::mirror_maker::CONNECTOR_CLASSES;
use crate::mirror_maker_config::MirrorMakerConfig;

// ============================================================================
// MirrorHerder - Herder with automatic connector configuration
// ============================================================================

/// MirrorHerder - Herder extension that automatically configures MirrorMaker connectors.
///
/// When this worker becomes the leader, it automatically creates or updates the
/// MirrorSourceConnector, MirrorHeartbeatConnector, and MirrorCheckpointConnector
/// based on the MirrorMaker configuration.
///
/// This automatic configuration ensures replication flows are established without
/// manual intervention.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.MirrorHerder
pub struct MirrorHerder {
    /// MirrorMaker configuration containing replication flow settings.
    /// Corresponds to Java: MirrorHerder.config (MirrorMakerConfig)
    config: MirrorMakerConfig,

    /// Source and target cluster pair for this herder.
    /// Corresponds to Java: MirrorHerder.sourceAndTarget (SourceAndTarget)
    source_and_target: SourceAndTarget,

    /// Whether this worker was leader in the previous rebalance.
    /// Used to detect leadership transition.
    /// Corresponds to Java: MirrorHerder.wasLeader (boolean)
    was_leader: AtomicBool,

    /// Whether the herder is started.
    started: AtomicBool,

    /// Whether the herder is stopping.
    stopping: AtomicBool,

    /// Whether this worker is currently the leader.
    is_leader: AtomicBool,

    /// In-memory connector configurations for tracking configured connectors.
    /// Corresponds to Java: DistributedHerder.configState.connectorConfig()
    connector_configs: RwLock<HashMap<String, HashMap<String, String>>>,
}

impl MirrorHerder {
    /// Creates a new MirrorHerder for the given source and target.
    ///
    /// # Arguments
    /// * `config` - MirrorMaker configuration
    /// * `source_and_target` - Source and target cluster pair
    ///
    /// Corresponds to Java: MirrorHerder(MirrorMakerConfig, SourceAndTarget, ...)
    pub fn new(config: MirrorMakerConfig, source_and_target: SourceAndTarget) -> Self {
        MirrorHerder {
            config,
            source_and_target,
            was_leader: AtomicBool::new(false),
            started: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            is_leader: AtomicBool::new(false),
            connector_configs: RwLock::new(HashMap::new()),
        }
    }

    /// Returns the source and target pair for this herder.
    pub fn source_and_target(&self) -> &SourceAndTarget {
        &self.source_and_target
    }

    /// Returns the MirrorMaker configuration.
    pub fn config(&self) -> &MirrorMakerConfig {
        &self.config
    }

    /// Check if the herder is started.
    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Check if the herder is stopping.
    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// Check if this worker is the leader.
    ///
    /// Corresponds to Java: DistributedHerder.isLeader()
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    /// Set the leader state (for testing or external control).
    pub fn set_leader(&self, is_leader: bool) {
        self.is_leader.store(is_leader, Ordering::SeqCst);
    }

    /// Handle rebalance success callback.
    ///
    /// This method is called after a successful rebalance. If this worker is
    /// now the leader and wasn't previously, it automatically configures
    /// the MirrorMaker connectors.
    ///
    /// Corresponds to Java: MirrorHerder.rebalanceSuccess()
    pub fn rebalance_success(&mut self) {
        if self.is_leader() {
            if !self.was_leader.load(Ordering::SeqCst) {
                println!(
                    "This node is now a leader for {}. Configuring connectors...",
                    self.source_and_target
                );
                self.configure_connectors();
            }
            self.was_leader.store(true, Ordering::SeqCst);
        } else {
            self.was_leader.store(false, Ordering::SeqCst);
        }
    }

    /// Configure all MirrorMaker connectors.
    ///
    /// Iterates through CONNECTOR_CLASSES and calls maybeConfigureConnector
    /// for each connector class.
    ///
    /// Corresponds to Java: MirrorHerder.configureConnectors()
    fn configure_connectors(&mut self) {
        for connector_class in CONNECTOR_CLASSES {
            self.maybe_configure_connector(connector_class);
        }
    }

    /// Configure a single connector if needed.
    ///
    /// Checks if the connector configuration differs from the desired configuration
    /// and updates it if necessary.
    ///
    /// # Arguments
    /// * `connector_class` - The full class name of the connector
    ///
    /// Corresponds to Java: MirrorHerder.maybeConfigureConnector(Class<?> connectorClass)
    fn maybe_configure_connector(&mut self, connector_class: &str) {
        let desired_config = self
            .config
            .connector_base_config(&self.source_and_target, connector_class);

        // Extract simple class name (last part after last dot)
        let connector_name = connector_class
            .rsplit('.')
            .next()
            .unwrap_or(connector_class);

        // Get current/actual config from connector_configs
        // Clone the config and release the lock immediately to avoid borrow conflict
        let actual_config = self
            .connector_configs
            .read()
            .unwrap()
            .get(connector_name)
            .cloned();

        // Check if we need to configure or update
        let needs_config = match &actual_config {
            None => true,
            Some(actual) => !actual.eq(&desired_config),
        };

        if needs_config {
            self.configure_connector(connector_name, desired_config);
        } else {
            println!(
                "This node is a leader for {} and configuration for {} is already up to date.",
                self.source_and_target, connector_name
            );
        }
    }

    /// Configure a connector with the given properties.
    ///
    /// Stores the connector configuration and logs the result.
    ///
    /// # Arguments
    /// * `connector_name` - Simple name of the connector
    /// * `connector_props` - Configuration properties
    ///
    /// Corresponds to Java: MirrorHerder.configureConnector(String, Map<String, String>)
    fn configure_connector(
        &mut self,
        connector_name: &str,
        connector_props: HashMap<String, String>,
    ) {
        // In a real implementation, this would call putConnectorConfig on the underlying herder
        // For this simplified version, we just store the config

        self.connector_configs
            .write()
            .unwrap()
            .insert(connector_name.to_string(), connector_props.clone());

        println!(
            "{} connector configured for {}.",
            connector_name, self.source_and_target
        );
    }

    /// Get connector config from internal storage.
    ///
    /// Corresponds to Java: DistributedHerder.configState.connectorConfig(String)
    pub fn connector_config(&self, connector_name: &str) -> Option<HashMap<String, String>> {
        self.connector_configs
            .read()
            .unwrap()
            .get(connector_name)
            .cloned()
    }

    /// Get list of configured connectors.
    pub fn configured_connectors(&self) -> Vec<String> {
        self.connector_configs
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }
}

impl std::fmt::Debug for MirrorHerder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorHerder")
            .field("source_and_target", &format!("{}", self.source_and_target))
            .field("was_leader", &self.was_leader.load(Ordering::SeqCst))
            .field("is_leader", &self.is_leader.load(Ordering::SeqCst))
            .field("started", &self.started.load(Ordering::SeqCst))
            .field("configured_connectors", &self.configured_connectors())
            .finish()
    }
}

// ============================================================================
// Herder trait implementation for MirrorHerder
// ============================================================================

impl Herder for MirrorHerder {
    fn start(&mut self) {
        if self.started.load(Ordering::SeqCst) {
            return;
        }
        self.started.store(true, Ordering::SeqCst);
        self.stopping.store(false, Ordering::SeqCst);

        // Simulate becoming leader after start (for testing)
        // In real implementation, this would be set during rebalance
    }

    fn stop(&mut self) {
        if self.stopping.load(Ordering::SeqCst) {
            return;
        }
        self.stopping.store(true, Ordering::SeqCst);
        self.started.store(false, Ordering::SeqCst);
    }

    fn is_ready(&self) -> bool {
        self.started.load(Ordering::SeqCst) && !self.stopping.load(Ordering::SeqCst)
    }

    fn health_check(&self, callback: Box<dyn Callback<()>>) {
        if self.is_ready() {
            callback.on_completion(());
        } else {
            callback.on_error("MirrorHerder not ready".to_string());
        }
    }

    fn connectors_async(&self, callback: Box<dyn Callback<Vec<String>>>) {
        callback.on_completion(self.configured_connectors());
    }

    fn connector_info_async(&self, conn_name: &str, callback: Box<dyn Callback<ConnectorInfo>>) {
        if let Some(config) = self.connector_config(conn_name) {
            callback.on_completion(ConnectorInfo {
                name: conn_name.to_string(),
                config,
                tasks: vec![], // Task info would be retrieved in real implementation
            });
        } else {
            callback.on_error(format!("Connector {} not found", conn_name));
        }
    }

    fn connector_config_async(
        &self,
        conn_name: &str,
        callback: Box<dyn Callback<HashMap<String, String>>>,
    ) {
        if let Some(config) = self.connector_config(conn_name) {
            callback.on_completion(config);
        } else {
            callback.on_error(format!("Connector {} not found", conn_name));
        }
    }

    fn put_connector_config(
        &self,
        conn_name: &str,
        config: HashMap<String, String>,
        _allow_replace: bool,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) {
        // In real implementation, this would validate and persist config
        self.connector_configs
            .write()
            .unwrap()
            .insert(conn_name.to_string(), config.clone());

        callback.on_completion(Created {
            created: true,
            info: ConnectorInfo {
                name: conn_name.to_string(),
                config,
                tasks: vec![],
            },
        });
    }

    fn put_connector_config_with_state(
        &self,
        conn_name: &str,
        config: HashMap<String, String>,
        _target_state: TargetState,
        _allow_replace: bool,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) {
        // Similar to put_connector_config but with target state
        self.connector_configs
            .write()
            .unwrap()
            .insert(conn_name.to_string(), config.clone());

        callback.on_completion(Created {
            created: true,
            info: ConnectorInfo {
                name: conn_name.to_string(),
                config,
                tasks: vec![],
            },
        });
    }

    fn patch_connector_config(
        &self,
        conn_name: &str,
        config_patch: HashMap<String, String>,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) {
        let mut configs = self.connector_configs.write().unwrap();
        if let Some(existing) = configs.get_mut(conn_name) {
            for (k, v) in config_patch {
                existing.insert(k, v);
            }
            let updated = existing.clone();
            callback.on_completion(Created {
                created: false, // Updated, not created
                info: ConnectorInfo {
                    name: conn_name.to_string(),
                    config: updated,
                    tasks: vec![],
                },
            });
        } else {
            callback.on_error(format!("Connector {} not found for patching", conn_name));
        }
    }

    fn delete_connector_config(
        &self,
        conn_name: &str,
        callback: Box<dyn Callback<Created<ConnectorInfo>>>,
    ) {
        let mut configs = self.connector_configs.write().unwrap();
        if let Some(config) = configs.remove(conn_name) {
            callback.on_completion(Created {
                created: false,
                info: ConnectorInfo {
                    name: conn_name.to_string(),
                    config,
                    tasks: vec![],
                },
            });
        } else {
            callback.on_error(format!("Connector {} not found", conn_name));
        }
    }

    fn request_task_reconfiguration(&self, _conn_name: &str) {
        // In real implementation, this would trigger task reconfiguration
    }

    fn task_configs_async(&self, _conn_name: &str, callback: Box<dyn Callback<Vec<TaskInfo>>>) {
        // Task configs would be retrieved in real implementation
        callback.on_completion(vec![]);
    }

    fn put_task_configs(
        &self,
        _conn_name: &str,
        _configs: Vec<HashMap<String, String>>,
        callback: Box<dyn Callback<()>>,
        _request_signature: InternalRequestSignature,
    ) {
        callback.on_error("MirrorHerder does not support putting task configs".to_string());
    }

    fn fence_zombie_source_tasks(
        &self,
        _conn_name: &str,
        callback: Box<dyn Callback<()>>,
        _request_signature: InternalRequestSignature,
    ) {
        callback.on_completion(());
    }

    fn connectors_sync(&self) -> Vec<String> {
        self.configured_connectors()
    }

    fn connector_info_sync(&self, conn_name: &str) -> ConnectorInfo {
        ConnectorInfo {
            name: conn_name.to_string(),
            config: self.connector_config(conn_name).unwrap_or_default(),
            tasks: vec![],
        }
    }

    fn connector_status(&self, conn_name: &str) -> ConnectorStateInfo {
        ConnectorStateInfo {
            name: conn_name.to_string(),
            connector: if self.connector_config(conn_name).is_some() {
                ConnectorState::Running
            } else {
                ConnectorState::Unassigned
            },
            tasks: vec![],
        }
    }

    fn connector_active_topics(&self, conn_name: &str) -> ActiveTopicsInfo {
        ActiveTopicsInfo {
            connector: conn_name.to_string(),
            topics: vec![],
        }
    }

    fn reset_connector_active_topics(&self, _conn_name: &str) {}

    fn status_backing_store(&self) -> Box<dyn StatusBackingStore> {
        Box::new(MirrorStatusBackingStore::new())
    }

    fn task_status(&self, id: &ConnectorTaskId) -> TaskStateInfo {
        TaskStateInfo {
            id: id.clone(),
            state: ConnectorState::Unassigned,
            worker_id: String::new(),
            trace: None,
        }
    }

    fn validate_connector_config_async(
        &self,
        connector_config: HashMap<String, String>,
        callback: Box<dyn Callback<ConfigInfos>>,
    ) {
        // Basic validation - check for connector.class
        let has_connector_class = connector_config.contains_key("connector.class");
        callback.on_completion(ConfigInfos {
            name: connector_config.get("name").cloned().unwrap_or_default(),
            error_count: if has_connector_class { 0 } else { 1 },
            group_count: 1,
            configs: vec![],
        });
    }

    fn validate_connector_config_async_with_log(
        &self,
        connector_config: HashMap<String, String>,
        callback: Box<dyn Callback<ConfigInfos>>,
        _do_log: bool,
    ) {
        self.validate_connector_config_async(connector_config, callback);
    }

    fn restart_task(&self, _id: &ConnectorTaskId, callback: Box<dyn Callback<()>>) {
        callback.on_error("MirrorHerder does not support restarting tasks".to_string());
    }

    fn restart_connector_async(&self, _conn_name: &str, callback: Box<dyn Callback<()>>) {
        callback.on_error("MirrorHerder does not support restarting connectors".to_string());
    }

    fn restart_connector_delayed(
        &self,
        _delay_ms: u64,
        _conn_name: &str,
        callback: Box<dyn Callback<()>>,
    ) -> Box<dyn HerderRequest> {
        callback.on_error("MirrorHerder does not support delayed restart".to_string());
        Box::new(MirrorHerderRequest::new())
    }

    fn pause_connector(&self, _connector: &str) {}

    fn resume_connector(&self, _connector: &str) {}

    fn stop_connector_async(&self, _connector: &str, callback: Box<dyn Callback<()>>) {
        callback.on_completion(());
    }

    fn restart_connector_and_tasks(
        &self,
        _request: &RestartRequest,
        callback: Box<dyn Callback<ConnectorStateInfo>>,
    ) {
        callback.on_error("MirrorHerder does not support restart with tasks".to_string());
    }

    fn plugins(&self) -> Box<dyn Plugins> {
        Box::new(MirrorPlugins::new())
    }

    fn kafka_cluster_id(&self) -> String {
        format!("mirror-{}", self.source_and_target)
    }

    fn connector_plugin_config(&self, _plugin_name: &str) -> Vec<ConfigKeyInfo> {
        vec![]
    }

    fn connector_plugin_config_with_version(
        &self,
        plugin_name: &str,
        _version: VersionRange,
    ) -> Vec<ConfigKeyInfo> {
        self.connector_plugin_config(plugin_name)
    }

    fn connector_offsets_async(
        &self,
        _conn_name: &str,
        callback: Box<dyn Callback<ConnectorOffsets>>,
    ) {
        callback.on_completion(ConnectorOffsets {
            offsets: HashMap::new(),
        });
    }

    fn alter_connector_offsets(
        &self,
        _conn_name: &str,
        _offsets: HashMap<HashMap<String, serde_json::Value>, HashMap<String, serde_json::Value>>,
        callback: Box<dyn Callback<Message>>,
    ) {
        callback.on_error("MirrorHerder does not support altering offsets".to_string());
    }

    fn reset_connector_offsets(&self, _conn_name: &str, callback: Box<dyn Callback<Message>>) {
        callback.on_error("MirrorHerder does not support resetting offsets".to_string());
    }

    fn logger_level(&self, logger: &str) -> LoggerLevel {
        LoggerLevel {
            logger: logger.to_string(),
            level: String::new(),
            effective_level: String::new(),
        }
    }

    fn all_logger_levels(&self) -> HashMap<String, LoggerLevel> {
        HashMap::new()
    }

    fn set_worker_logger_level(&self, _namespace: &str, _level: &str) -> Vec<String> {
        vec![]
    }

    fn set_cluster_logger_level(&self, _namespace: &str, _level: &str) {}

    fn connect_metrics(&self) -> Box<dyn common_trait::herder::ConnectMetrics> {
        Box::new(MirrorConnectMetrics::new())
    }
}

// ============================================================================
// MirrorStatusBackingStore - Status storage for MirrorHerder
// ============================================================================

/// MirrorStatusBackingStore - Status backing store implementation.
struct MirrorStatusBackingStore {
    connector_states: RwLock<HashMap<String, ConnectorState>>,
    task_states: RwLock<HashMap<ConnectorTaskId, TaskStateInfo>>,
}

impl MirrorStatusBackingStore {
    fn new() -> Self {
        MirrorStatusBackingStore {
            connector_states: RwLock::new(HashMap::new()),
            task_states: RwLock::new(HashMap::new()),
        }
    }
}

impl StatusBackingStore for MirrorStatusBackingStore {
    fn get_connector_state(&self, connector: &str) -> Option<ConnectorState> {
        self.connector_states
            .read()
            .unwrap()
            .get(connector)
            .copied()
    }

    fn get_task_state(&self, id: &ConnectorTaskId) -> Option<TaskStateInfo> {
        self.task_states.read().unwrap().get(id).cloned()
    }

    fn put_connector_state(&self, connector: &str, state: ConnectorState) {
        self.connector_states
            .write()
            .unwrap()
            .insert(connector.to_string(), state);
    }

    fn put_task_state(&self, id: &ConnectorTaskId, state: TaskStateInfo) {
        self.task_states.write().unwrap().insert(id.clone(), state);
    }
}

// ============================================================================
// MirrorPlugins - Plugins for MirrorHerder
// ============================================================================

/// MirrorPlugins - Plugin discovery for MirrorMaker connectors.
struct MirrorPlugins {}

impl MirrorPlugins {
    fn new() -> Self {
        MirrorPlugins {}
    }
}

impl Plugins for MirrorPlugins {
    fn connector_plugins(&self) -> Vec<String> {
        CONNECTOR_CLASSES.iter().map(|s| s.to_string()).collect()
    }

    fn converter_plugins(&self) -> Vec<String> {
        vec!["org.apache.kafka.connect.converters.ByteArrayConverter".to_string()]
    }

    fn transformation_plugins(&self) -> Vec<String> {
        vec![]
    }

    fn connector_plugin_desc(&self, class_name: &str) -> Option<PluginDesc> {
        if CONNECTOR_CLASSES.contains(&class_name) {
            Some(PluginDesc {
                class_name: class_name.to_string(),
                plugin_type: if class_name.contains("Source") {
                    common_trait::herder::PluginType::Source
                } else {
                    common_trait::herder::PluginType::Sink
                },
                version: "1.0.0".to_string(),
                documentation: "MirrorMaker connector".to_string(),
            })
        } else {
            None
        }
    }

    fn converter_plugin_desc(&self, class_name: &str) -> Option<PluginDesc> {
        Some(PluginDesc {
            class_name: class_name.to_string(),
            plugin_type: common_trait::herder::PluginType::Converter,
            version: "1.0.0".to_string(),
            documentation: "Converter plugin".to_string(),
        })
    }
}

// ============================================================================
// MirrorHerderRequest - Request handle for async operations
// ============================================================================

/// MirrorHerderRequest - Request handle implementation.
struct MirrorHerderRequest {
    cancelled: AtomicBool,
    completed: AtomicBool,
}

impl MirrorHerderRequest {
    fn new() -> Self {
        MirrorHerderRequest {
            cancelled: AtomicBool::new(false),
            completed: AtomicBool::new(false),
        }
    }
}

impl HerderRequest for MirrorHerderRequest {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }
}

// ============================================================================
// MirrorConnectMetrics - Metrics for MirrorHerder
// ============================================================================

/// MirrorConnectMetrics - Connect metrics implementation.
struct MirrorConnectMetrics {}

impl MirrorConnectMetrics {
    fn new() -> Self {
        MirrorConnectMetrics {}
    }
}

impl common_trait::herder::ConnectMetrics for MirrorConnectMetrics {
    fn registry(&self) -> &dyn common_trait::herder::MetricsRegistry {
        &MirrorMetricsRegistry {}
    }

    fn stop(&self) {}
}

/// MirrorMetricsRegistry - Metrics registry implementation.
struct MirrorMetricsRegistry {}

impl common_trait::herder::MetricsRegistry for MirrorMetricsRegistry {
    fn worker_group_name(&self) -> &str {
        "mirror-maker-group"
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> MirrorMakerConfig {
        let mut props = HashMap::new();
        props.insert("clusters".to_string(), "A, B".to_string());
        props.insert("A.bootstrap.servers".to_string(), "aaa:9092".to_string());
        props.insert("B.bootstrap.servers".to_string(), "bbb:9092".to_string());
        props.insert("A->B.enabled".to_string(), "true".to_string());
        MirrorMakerConfig::new(props)
    }

    #[test]
    fn test_mirror_herder_new() {
        let config = create_test_config();
        let source_and_target = SourceAndTarget::new("A", "B");
        let herder = MirrorHerder::new(config, source_and_target.clone());

        assert_eq!(herder.source_and_target(), &source_and_target);
        assert!(!herder.is_leader());
        assert!(!herder.was_leader.load(Ordering::SeqCst));
    }

    #[test]
    fn test_mirror_herder_start_stop() {
        let config = create_test_config();
        let source_and_target = SourceAndTarget::new("A", "B");
        let mut herder = MirrorHerder::new(config, source_and_target);

        herder.start();
        assert!(herder.is_started());
        assert!(herder.is_ready());

        herder.stop();
        assert!(herder.is_stopping());
        assert!(!herder.is_ready());
    }

    #[test]
    fn test_rebalance_success_not_leader() {
        let config = create_test_config();
        let source_and_target = SourceAndTarget::new("A", "B");
        let mut herder = MirrorHerder::new(config, source_and_target);

        // Not leader - should not configure connectors
        herder.rebalance_success();

        assert!(herder.configured_connectors().is_empty());
        assert!(!herder.was_leader.load(Ordering::SeqCst));
    }

    #[test]
    fn test_rebalance_success_becomes_leader() {
        let config = create_test_config();
        let source_and_target = SourceAndTarget::new("A", "B");
        let mut herder = MirrorHerder::new(config, source_and_target);

        // Set as leader
        herder.set_leader(true);

        // Trigger rebalance success - should configure connectors
        herder.rebalance_success();

        // Should have configured all 3 connectors
        assert_eq!(herder.configured_connectors().len(), 3);
        assert!(herder.was_leader.load(Ordering::SeqCst));
    }

    #[test]
    fn test_rebalance_success_still_leader() {
        let config = create_test_config();
        let source_and_target = SourceAndTarget::new("A", "B");
        let mut herder = MirrorHerder::new(config, source_and_target);

        // First rebalance as leader
        herder.set_leader(true);
        herder.rebalance_success();

        let configured_count = herder.configured_connectors().len();

        // Second rebalance as leader - should not reconfigure
        herder.rebalance_success();

        // Should still have same number of connectors (no reconfiguration)
        assert_eq!(herder.configured_connectors().len(), configured_count);
    }

    #[test]
    fn test_rebalance_success_loses_leader() {
        let config = create_test_config();
        let source_and_target = SourceAndTarget::new("A", "B");
        let mut herder = MirrorHerder::new(config, source_and_target);

        // Become leader first
        herder.set_leader(true);
        herder.rebalance_success();
        assert!(herder.was_leader.load(Ordering::SeqCst));

        // Lose leadership
        herder.set_leader(false);
        herder.rebalance_success();

        // was_leader should be reset
        assert!(!herder.was_leader.load(Ordering::SeqCst));
        // But configured connectors should remain
        assert_eq!(herder.configured_connectors().len(), 3);
    }

    #[test]
    fn test_connector_config() {
        let config = create_test_config();
        let source_and_target = SourceAndTarget::new("A", "B");
        let mut herder = MirrorHerder::new(config, source_and_target);

        // Configure as leader
        herder.set_leader(true);
        herder.rebalance_success();

        // Check each connector was configured
        for connector_class in CONNECTOR_CLASSES {
            let connector_name = connector_class.rsplit('.').next().unwrap();
            let conn_config = herder.connector_config(connector_name);
            assert!(conn_config.is_some());

            // Verify config has required fields
            let cfg = conn_config.unwrap();
            assert!(cfg.contains_key("connector.class"));
            assert!(cfg.contains_key("source.cluster.alias"));
            assert!(cfg.contains_key("target.cluster.alias"));
        }
    }

    #[test]
    fn test_herder_trait_connectors_async() {
        let config = create_test_config();
        let source_and_target = SourceAndTarget::new("A", "B");
        let mut herder = MirrorHerder::new(config, source_and_target);

        // Configure as leader
        herder.set_leader(true);
        herder.rebalance_success();

        // Test Herder trait method
        use common_trait::herder::Callback;

        struct TestCallback {
            result: Arc<RwLock<Option<Vec<String>>>>,
        }

        impl Callback<Vec<String>> for TestCallback {
            fn on_completion(&self, result: Vec<String>) {
                *self.result.write().unwrap() = Some(result);
            }
            fn on_error(&self, _error: String) {}
        }

        let result = Arc::new(RwLock::new(None));
        let callback = TestCallback {
            result: result.clone(),
        };
        herder.connectors_async(Box::new(callback));

        let connectors = result.read().unwrap().clone().unwrap();
        assert_eq!(connectors.len(), 3);
    }

    #[test]
    fn test_debug_format() {
        let config = create_test_config();
        let source_and_target = SourceAndTarget::new("A", "B");
        let mut herder = MirrorHerder::new(config, source_and_target);

        herder.set_leader(true);
        herder.rebalance_success();

        let debug_str = format!("{:?}", herder);
        assert!(debug_str.contains("MirrorHerder"));
        assert!(debug_str.contains("A->B"));
        assert!(debug_str.contains("is_leader: true"));
        assert!(debug_str.contains("MirrorSourceConnector"));
    }
}
