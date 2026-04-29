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

//! MirrorMaker - Entry point for "MirrorMaker 2.0".
//!
//! MirrorMaker runs a set of Connectors between multiple clusters, in order to
//! replicate data, configuration, ACL rules, and consumer group state.
//!
//! Configuration is via a top-level "mm2.properties" file, which supports per-cluster
//! and per-replication sub-configs. Each source->target replication must be explicitly enabled.
//!
//! Example configuration:
//! ```text
//!   clusters = primary, backup
//!   primary.bootstrap.servers = vip1:9092
//!   backup.bootstrap.servers = vip2:9092
//!   primary->backup.enabled = true
//!   backup->primary.enabled = true
//! ```
//!
//! Corresponds to Java: org.apache.kafka.connect.mirror.MirrorMaker (~370 lines)

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use common_trait::herder::{ConnectorStateInfo, Herder, TaskInfo};
use connect_mirror_client::SourceAndTarget;

use crate::mirror_maker_config::MirrorMakerConfig;

// ============================================================================
// Configuration Constants - 1:1 correspondence with Java MirrorMaker
// ============================================================================

/// Shutdown timeout in seconds.
/// Corresponds to Java: MirrorMaker.SHUTDOWN_TIMEOUT_SECONDS (60L)
const SHUTDOWN_TIMEOUT_SECONDS: u64 = 60;

/// List of connector classes used by MirrorMaker.
/// Corresponds to Java: MirrorMaker.CONNECTOR_CLASSES
pub const CONNECTOR_CLASSES: &[&str] = &[
    "org.apache.kafka.connect.mirror.MirrorSourceConnector",
    "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
    "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
];

// ============================================================================
// MockHerder - Simplified Herder implementation for MirrorMaker
// ============================================================================

/// MockHerder - Simplified Herder implementation for testing MirrorMaker.
///
/// This is a simplified version that simulates Herder behavior without
/// the full Worker infrastructure. Used for basic MirrorMaker testing.
///
/// In production, MirrorMaker would use MirrorHerder (DistributedHerder-based).
pub struct MockHerder {
    /// Source and target cluster pair.
    source_and_target: SourceAndTarget,
    /// Whether the herder is started.
    started: AtomicBool,
    /// Whether the herder is stopping.
    stopping: AtomicBool,
}

impl MockHerder {
    /// Creates a new MockHerder for the given source and target.
    pub fn new(source_and_target: SourceAndTarget) -> Self {
        MockHerder {
            source_and_target,
            started: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
        }
    }

    /// Returns the source and target pair.
    pub fn source_and_target(&self) -> &SourceAndTarget {
        &self.source_and_target
    }

    /// Check if the herder is started.
    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Check if the herder is stopping.
    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }
}

impl Herder for MockHerder {
    fn start(&mut self) {
        if self.started.load(Ordering::SeqCst) {
            return;
        }
        self.started.store(true, Ordering::SeqCst);
        self.stopping.store(false, Ordering::SeqCst);
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

    fn health_check(&self, callback: Box<dyn common_trait::herder::Callback<()>>) {
        if self.is_ready() {
            callback.on_completion(());
        } else {
            callback.on_error("Herder not ready".to_string());
        }
    }

    fn connectors_async(&self, callback: Box<dyn common_trait::herder::Callback<Vec<String>>>) {
        callback.on_completion(vec![]);
    }

    fn connector_info_async(
        &self,
        _conn_name: &str,
        callback: Box<dyn common_trait::herder::Callback<common_trait::herder::ConnectorInfo>>,
    ) {
        callback.on_error("MockHerder does not support connector info".to_string());
    }

    fn connector_config_async(
        &self,
        _conn_name: &str,
        callback: Box<dyn common_trait::herder::Callback<HashMap<String, String>>>,
    ) {
        callback.on_error("MockHerder does not support connector config".to_string());
    }

    fn put_connector_config(
        &self,
        _conn_name: &str,
        _config: HashMap<String, String>,
        _allow_replace: bool,
        callback: Box<
            dyn common_trait::herder::Callback<
                common_trait::herder::Created<common_trait::herder::ConnectorInfo>,
            >,
        >,
    ) {
        callback.on_error("MockHerder does not support putting connector config".to_string());
    }

    fn put_connector_config_with_state(
        &self,
        _conn_name: &str,
        _config: HashMap<String, String>,
        _target_state: common_trait::herder::TargetState,
        _allow_replace: bool,
        callback: Box<
            dyn common_trait::herder::Callback<
                common_trait::herder::Created<common_trait::herder::ConnectorInfo>,
            >,
        >,
    ) {
        callback.on_error(
            "MockHerder does not support putting connector config with state".to_string(),
        );
    }

    fn patch_connector_config(
        &self,
        _conn_name: &str,
        _config_patch: HashMap<String, String>,
        callback: Box<
            dyn common_trait::herder::Callback<
                common_trait::herder::Created<common_trait::herder::ConnectorInfo>,
            >,
        >,
    ) {
        callback.on_error("MockHerder does not support patching connector config".to_string());
    }

    fn delete_connector_config(
        &self,
        _conn_name: &str,
        callback: Box<
            dyn common_trait::herder::Callback<
                common_trait::herder::Created<common_trait::herder::ConnectorInfo>,
            >,
        >,
    ) {
        callback.on_error("MockHerder does not support deleting connector config".to_string());
    }

    fn request_task_reconfiguration(&self, _conn_name: &str) {}

    fn task_configs_async(
        &self,
        _conn_name: &str,
        callback: Box<dyn common_trait::herder::Callback<Vec<TaskInfo>>>,
    ) {
        callback.on_completion(vec![]);
    }

    fn put_task_configs(
        &self,
        _conn_name: &str,
        _configs: Vec<HashMap<String, String>>,
        callback: Box<dyn common_trait::herder::Callback<()>>,
        _request_signature: common_trait::herder::InternalRequestSignature,
    ) {
        callback.on_error("MockHerder does not support putting task configs".to_string());
    }

    fn fence_zombie_source_tasks(
        &self,
        _conn_name: &str,
        callback: Box<dyn common_trait::herder::Callback<()>>,
        _request_signature: common_trait::herder::InternalRequestSignature,
    ) {
        callback.on_completion(());
    }

    fn connectors_sync(&self) -> Vec<String> {
        vec![]
    }

    fn connector_info_sync(&self, conn_name: &str) -> common_trait::herder::ConnectorInfo {
        common_trait::herder::ConnectorInfo {
            name: conn_name.to_string(),
            config: HashMap::new(),
            tasks: vec![],
        }
    }

    fn connector_status(&self, conn_name: &str) -> ConnectorStateInfo {
        ConnectorStateInfo {
            name: conn_name.to_string(),
            connector: common_trait::herder::ConnectorState::Unassigned,
            tasks: vec![],
        }
    }

    fn connector_active_topics(&self, conn_name: &str) -> common_trait::herder::ActiveTopicsInfo {
        common_trait::herder::ActiveTopicsInfo {
            connector: conn_name.to_string(),
            topics: vec![],
        }
    }

    fn reset_connector_active_topics(&self, _conn_name: &str) {}

    fn status_backing_store(&self) -> Box<dyn common_trait::herder::StatusBackingStore> {
        Box::new(MockStatusBackingStore::new())
    }

    fn task_status(
        &self,
        id: &common_trait::herder::ConnectorTaskId,
    ) -> common_trait::herder::TaskStateInfo {
        common_trait::herder::TaskStateInfo {
            id: id.clone(),
            state: common_trait::herder::ConnectorState::Unassigned,
            worker_id: String::new(),
            trace: None,
        }
    }

    fn validate_connector_config_async(
        &self,
        _connector_config: HashMap<String, String>,
        callback: Box<dyn common_trait::herder::Callback<common_trait::herder::ConfigInfos>>,
    ) {
        callback.on_completion(common_trait::herder::ConfigInfos {
            name: String::new(),
            error_count: 0,
            group_count: 0,
            configs: vec![],
        });
    }

    fn validate_connector_config_async_with_log(
        &self,
        connector_config: HashMap<String, String>,
        callback: Box<dyn common_trait::herder::Callback<common_trait::herder::ConfigInfos>>,
        _do_log: bool,
    ) {
        self.validate_connector_config_async(connector_config, callback);
    }

    fn restart_task(
        &self,
        _id: &common_trait::herder::ConnectorTaskId,
        callback: Box<dyn common_trait::herder::Callback<()>>,
    ) {
        callback.on_error("MockHerder does not support restarting tasks".to_string());
    }

    fn restart_connector_async(
        &self,
        _conn_name: &str,
        callback: Box<dyn common_trait::herder::Callback<()>>,
    ) {
        callback.on_error("MockHerder does not support restarting connectors".to_string());
    }

    fn restart_connector_delayed(
        &self,
        _delay_ms: u64,
        _conn_name: &str,
        callback: Box<dyn common_trait::herder::Callback<()>>,
    ) -> Box<dyn common_trait::herder::HerderRequest> {
        callback.on_error("MockHerder does not support delayed restart".to_string());
        Box::new(MockHerderRequest::new())
    }

    fn pause_connector(&self, _connector: &str) {}

    fn resume_connector(&self, _connector: &str) {}

    fn stop_connector_async(
        &self,
        _connector: &str,
        callback: Box<dyn common_trait::herder::Callback<()>>,
    ) {
        callback.on_completion(());
    }

    fn restart_connector_and_tasks(
        &self,
        _request: &common_trait::herder::RestartRequest,
        callback: Box<dyn common_trait::herder::Callback<ConnectorStateInfo>>,
    ) {
        callback.on_error("MockHerder does not support restart with tasks".to_string());
    }

    fn plugins(&self) -> Box<dyn common_trait::herder::Plugins> {
        Box::new(MockPlugins::new())
    }

    fn kafka_cluster_id(&self) -> String {
        String::new()
    }

    fn connector_plugin_config(
        &self,
        _plugin_name: &str,
    ) -> Vec<common_trait::herder::ConfigKeyInfo> {
        vec![]
    }

    fn connector_plugin_config_with_version(
        &self,
        plugin_name: &str,
        _version: common_trait::herder::VersionRange,
    ) -> Vec<common_trait::herder::ConfigKeyInfo> {
        self.connector_plugin_config(plugin_name)
    }

    fn connector_offsets_async(
        &self,
        _conn_name: &str,
        callback: Box<dyn common_trait::herder::Callback<common_trait::herder::ConnectorOffsets>>,
    ) {
        callback.on_completion(common_trait::herder::ConnectorOffsets {
            offsets: HashMap::new(),
        });
    }

    fn alter_connector_offsets(
        &self,
        _conn_name: &str,
        _offsets: HashMap<HashMap<String, serde_json::Value>, HashMap<String, serde_json::Value>>,
        callback: Box<dyn common_trait::herder::Callback<common_trait::herder::Message>>,
    ) {
        callback.on_error("MockHerder does not support altering offsets".to_string());
    }

    fn reset_connector_offsets(
        &self,
        _conn_name: &str,
        callback: Box<dyn common_trait::herder::Callback<common_trait::herder::Message>>,
    ) {
        callback.on_error("MockHerder does not support resetting offsets".to_string());
    }

    fn logger_level(&self, logger: &str) -> common_trait::herder::LoggerLevel {
        common_trait::herder::LoggerLevel {
            logger: logger.to_string(),
            level: String::new(),
            effective_level: String::new(),
        }
    }

    fn all_logger_levels(&self) -> HashMap<String, common_trait::herder::LoggerLevel> {
        HashMap::new()
    }

    fn set_worker_logger_level(&self, _namespace: &str, _level: &str) -> Vec<String> {
        vec![]
    }

    fn set_cluster_logger_level(&self, _namespace: &str, _level: &str) {}

    fn connect_metrics(&self) -> Box<dyn common_trait::herder::ConnectMetrics> {
        Box::new(MockConnectMetrics::new())
    }
}

/// MockStatusBackingStore - Mock implementation for testing.
struct MockStatusBackingStore {
    connector_states: RwLock<HashMap<String, common_trait::herder::ConnectorState>>,
    task_states:
        RwLock<HashMap<common_trait::herder::ConnectorTaskId, common_trait::herder::TaskStateInfo>>,
}

impl MockStatusBackingStore {
    fn new() -> Self {
        MockStatusBackingStore {
            connector_states: RwLock::new(HashMap::new()),
            task_states: RwLock::new(HashMap::new()),
        }
    }
}

impl common_trait::herder::StatusBackingStore for MockStatusBackingStore {
    fn get_connector_state(&self, connector: &str) -> Option<common_trait::herder::ConnectorState> {
        self.connector_states
            .read()
            .unwrap()
            .get(connector)
            .copied()
    }

    fn get_task_state(
        &self,
        id: &common_trait::herder::ConnectorTaskId,
    ) -> Option<common_trait::herder::TaskStateInfo> {
        self.task_states.read().unwrap().get(id).cloned()
    }

    fn put_connector_state(&self, connector: &str, state: common_trait::herder::ConnectorState) {
        self.connector_states
            .write()
            .unwrap()
            .insert(connector.to_string(), state);
    }

    fn put_task_state(
        &self,
        id: &common_trait::herder::ConnectorTaskId,
        state: common_trait::herder::TaskStateInfo,
    ) {
        self.task_states.write().unwrap().insert(id.clone(), state);
    }
}

/// MockPlugins - Mock implementation for testing.
struct MockPlugins {}

impl MockPlugins {
    fn new() -> Self {
        MockPlugins {}
    }
}

impl common_trait::herder::Plugins for MockPlugins {
    fn connector_plugins(&self) -> Vec<String> {
        CONNECTOR_CLASSES.iter().map(|s| s.to_string()).collect()
    }

    fn converter_plugins(&self) -> Vec<String> {
        vec!["org.apache.kafka.connect.storage.ByteArrayConverter".to_string()]
    }

    fn transformation_plugins(&self) -> Vec<String> {
        vec![]
    }

    fn connector_plugin_desc(&self, class_name: &str) -> Option<common_trait::herder::PluginDesc> {
        if CONNECTOR_CLASSES.contains(&class_name) {
            Some(common_trait::herder::PluginDesc {
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

    fn converter_plugin_desc(&self, class_name: &str) -> Option<common_trait::herder::PluginDesc> {
        Some(common_trait::herder::PluginDesc {
            class_name: class_name.to_string(),
            plugin_type: common_trait::herder::PluginType::Converter,
            version: "1.0.0".to_string(),
            documentation: "Converter plugin".to_string(),
        })
    }
}

/// MockHerderRequest - Mock implementation for testing.
struct MockHerderRequest {
    cancelled: AtomicBool,
    completed: AtomicBool,
}

impl MockHerderRequest {
    fn new() -> Self {
        MockHerderRequest {
            cancelled: AtomicBool::new(false),
            completed: AtomicBool::new(false),
        }
    }
}

impl common_trait::herder::HerderRequest for MockHerderRequest {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }
}

/// MockConnectMetrics - Mock implementation for testing.
struct MockConnectMetrics {}

impl MockConnectMetrics {
    fn new() -> Self {
        MockConnectMetrics {}
    }
}

impl common_trait::herder::ConnectMetrics for MockConnectMetrics {
    fn registry(&self) -> &dyn common_trait::herder::MetricsRegistry {
        &MockMetricsRegistry {}
    }

    fn stop(&self) {}
}

/// MockMetricsRegistry - Mock implementation for testing.
struct MockMetricsRegistry {}

impl common_trait::herder::MetricsRegistry for MockMetricsRegistry {
    fn worker_group_name(&self) -> &str {
        "mock-group"
    }
}

/// SimpleTaskInfoCallback - A callback that captures task info results.
/// Uses Arc internally so Clone shares storage.
struct SimpleTaskInfoCallback {
    state: Arc<RwLock<Option<Vec<TaskInfo>>>>,
}

impl Clone for SimpleTaskInfoCallback {
    fn clone(&self) -> Self {
        SimpleTaskInfoCallback {
            state: self.state.clone(),
        }
    }
}

impl SimpleTaskInfoCallback {
    fn new() -> Self {
        SimpleTaskInfoCallback {
            state: Arc::new(RwLock::new(None)),
        }
    }

    fn result(&self) -> Option<Vec<TaskInfo>> {
        self.state.read().unwrap().clone()
    }
}

impl common_trait::herder::Callback<Vec<TaskInfo>> for SimpleTaskInfoCallback {
    fn on_completion(&self, result: Vec<TaskInfo>) {
        *self.state.write().unwrap() = Some(result);
    }

    fn on_error(&self, _error: String) {}
}

// ============================================================================
// MirrorMaker
// ============================================================================

/// MirrorMaker - Entry point for "MirrorMaker 2.0".
///
/// MirrorMaker runs a set of Connectors between multiple clusters, in order to
/// replicate data, configuration, ACL rules, and consumer group state.
///
/// Thread safety: Uses atomic flags for lifecycle state. Uses RwLock for herders map.
///
/// Corresponds to Java: org.apache.kafka.connect.mirror.MirrorMaker (~370 lines)
pub struct MirrorMaker {
    /// Herder instances keyed by SourceAndTarget.
    /// Corresponds to Java: MirrorMaker.herders (Map<SourceAndTarget, Herder>)
    herders: RwLock<HashMap<SourceAndTarget, Arc<RwLock<dyn Herder + Send + Sync>>>>,

    /// Whether the MirrorMaker has been started.
    /// Used to prevent double start.
    started: AtomicBool,

    /// Start latch counter - counts remaining herders to start.
    /// Corresponds to Java: MirrorMaker.startLatch (CountDownLatch)
    start_latch: AtomicUsize,

    /// Stop latch counter - counts remaining herders to stop.
    /// Corresponds to Java: MirrorMaker.stopLatch (CountDownLatch)
    stop_latch: AtomicUsize,

    /// Shutdown flag - indicates if shutdown has been initiated.
    /// Corresponds to Java: MirrorMaker.shutdown (AtomicBoolean)
    shutdown: AtomicBool,

    /// MirrorMaker configuration.
    /// Corresponds to Java: MirrorMaker.config (MirrorMakerConfig)
    config: MirrorMakerConfig,

    /// Target clusters for this node.
    /// Corresponds to Java: MirrorMaker.clusters (Set<String>)
    clusters: HashSet<String>,
}

impl MirrorMaker {
    /// Creates a new MirrorMaker instance.
    ///
    /// # Arguments
    /// * `config` - MirrorMaker configuration from mm2.properties file
    /// * `clusters` - Target clusters for this node (must match cluster aliases in config).
    ///                If None or empty, uses all clusters in the config.
    ///
    /// Corresponds to Java: MirrorMaker(MirrorMakerConfig config, List<String> clusters, Time time)
    pub fn new(config: MirrorMakerConfig, clusters: Option<Vec<String>>) -> Self {
        // Determine target clusters
        let target_clusters = if let Some(clusters_list) = clusters {
            if clusters_list.is_empty() {
                config.clusters()
            } else {
                clusters_list.into_iter().collect()
            }
        } else {
            // Default to all clusters
            config.clusters()
        };

        // Get herder pairs - filter by target clusters
        let herder_pairs: Vec<SourceAndTarget> = config
            .cluster_pairs()
            .into_iter()
            .filter(|pair| target_clusters.contains(pair.target()))
            .collect();

        if herder_pairs.is_empty() {
            panic!(
                "No source->target replication flows. Target clusters: {:?}",
                target_clusters
            );
        }

        // Create herders for each pair
        let mut herders_map = HashMap::new();
        for source_and_target in herder_pairs {
            let herder = Self::create_herder(&config, source_and_target.clone());
            herders_map.insert(source_and_target, herder);
        }

        MirrorMaker {
            herders: RwLock::new(herders_map),
            started: AtomicBool::new(false),
            start_latch: AtomicUsize::new(0),
            stop_latch: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
            config,
            clusters: target_clusters,
        }
    }

    /// Creates a herder for the given source and target.
    ///
    /// In this simplified version, creates a MockHerder.
    /// In production, would create MirrorHerder with full Worker infrastructure.
    ///
    /// Corresponds to Java: MirrorMaker.addHerder(SourceAndTarget sourceAndTarget)
    fn create_herder(
        _config: &MirrorMakerConfig,
        source_and_target: SourceAndTarget,
    ) -> Arc<RwLock<dyn Herder + Send + Sync>> {
        // Create MockHerder for testing
        Arc::new(RwLock::new(MockHerder::new(source_and_target)))
    }

    /// Starts all herders.
    ///
    /// Starts each herder and initializes internal REST resources if enabled.
    /// The start latch is used to track startup completion.
    ///
    /// Corresponds to Java: MirrorMaker.start()
    pub fn start(&mut self) {
        if self.started.load(Ordering::SeqCst) {
            panic!("MirrorMaker instance already started");
        }

        let herders = self.herders.read().unwrap();
        let herder_count = herders.len();
        self.started.store(true, Ordering::SeqCst);
        self.start_latch.store(herder_count, Ordering::SeqCst);
        self.stop_latch.store(herder_count, Ordering::SeqCst);

        // Start each herder
        for herder_arc in herders.values() {
            let mut herder = herder_arc.write().unwrap();
            herder.start();
            self.start_latch.fetch_sub(1, Ordering::SeqCst);
        }

        // In Java: internal REST server initialization would happen here
        // For simplified version, skip internal REST server

        println!("Kafka MirrorMaker started with {} herders", herder_count);
    }

    /// Stops all herders.
    ///
    /// If shutdown is already initiated, returns immediately.
    /// Stops internal REST server (if enabled) and then stops each herder.
    ///
    /// Corresponds to Java: MirrorMaker.stop()
    pub fn stop(&mut self) {
        let was_shutting_down = self.shutdown.swap(true, Ordering::SeqCst);
        if was_shutting_down {
            return;
        }

        println!("Kafka MirrorMaker stopping");

        // In Java: internal REST server stop would happen here
        // For simplified version, skip internal REST server

        let herders = self.herders.read().unwrap();
        for herder_arc in herders.values() {
            let mut herder = herder_arc.write().unwrap();
            herder.stop();
            self.stop_latch.fetch_sub(1, Ordering::SeqCst);
        }

        println!("Kafka MirrorMaker stopped");
    }

    /// Waits for all herders to stop.
    ///
    /// Blocks until the stop latch reaches zero or timeout elapses.
    /// Returns true if all herders stopped, false if timeout elapsed.
    ///
    /// Corresponds to Java: MirrorMaker.awaitStop()
    pub fn await_stop(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while self.stop_latch.load(Ordering::SeqCst) > 0 {
            if std::time::Instant::now() >= deadline {
                eprintln!("Timed out waiting for MirrorMaker to stop");
                return false;
            }
            thread::sleep(Duration::from_millis(100));
        }
        true
    }

    /// Waits indefinitely for all herders to stop.
    ///
    /// Corresponds to Java: MirrorMaker.awaitStop() (blocking version)
    pub fn await_stop_blocking(&self) {
        while self.stop_latch.load(Ordering::SeqCst) > 0 {
            thread::sleep(Duration::from_millis(100));
        }
    }

    /// Returns the target clusters for this MirrorMaker instance.
    ///
    /// Corresponds to Java: MirrorMaker.clusters()
    pub fn clusters(&self) -> &HashSet<String> {
        &self.clusters
    }

    /// Returns the number of herders.
    ///
    /// Corresponds to Java: MirrorMaker.herders.size()
    pub fn herder_count(&self) -> usize {
        self.herders.read().unwrap().len()
    }

    /// Returns whether shutdown has been initiated.
    ///
    /// Corresponds to Java: MirrorMaker.shutdown.get()
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Returns the MirrorMaker configuration.
    ///
    /// Corresponds to Java: MirrorMaker.config
    pub fn config(&self) -> &MirrorMakerConfig {
        &self.config
    }

    /// Checks if a herder exists for the given source and target.
    ///
    /// Corresponds to Java: MirrorMaker.checkHerder(SourceAndTarget)
    fn check_herder(&self, source_and_target: &SourceAndTarget) -> bool {
        self.herders.read().unwrap().contains_key(source_and_target)
    }

    /// Returns the connector status for a specific herder.
    ///
    /// # Arguments
    /// * `source_and_target` - The source and target cluster pair
    /// * `connector` - The connector name
    ///
    /// Corresponds to Java: MirrorMaker.connectorStatus(SourceAndTarget, String)
    pub fn connector_status(
        &self,
        source_and_target: &SourceAndTarget,
        connector: &str,
    ) -> Option<ConnectorStateInfo> {
        if !self.check_herder(source_and_target) {
            return None;
        }

        let herders = self.herders.read().unwrap();
        if let Some(herder_arc) = herders.get(source_and_target) {
            let herder = herder_arc.read().unwrap();
            Some(herder.connector_status(connector))
        } else {
            None
        }
    }

    /// Returns task configs for a connector in a specific herder.
    ///
    /// # Arguments
    /// * `source_and_target` - The source and target cluster pair
    /// * `connector` - The connector name
    ///
    /// Corresponds to Java: MirrorMaker.taskConfigs(SourceAndTarget, String, Callback)
    pub fn task_configs(
        &self,
        source_and_target: &SourceAndTarget,
        connector: &str,
    ) -> Option<Vec<TaskInfo>> {
        if !self.check_herder(source_and_target) {
            return None;
        }

        let herders = self.herders.read().unwrap();
        if let Some(herder_arc) = herders.get(source_and_target) {
            let herder = herder_arc.read().unwrap();
            // Use a simple callback that captures the result
            let callback = SimpleTaskInfoCallback::new();
            herder.task_configs_async(connector, Box::new(callback.clone()));
            callback.result()
        } else {
            None
        }
    }

    /// Returns the list of herder pairs (source->target).
    ///
    /// Useful for iterating over all herders.
    pub fn herder_pairs(&self) -> Vec<SourceAndTarget> {
        self.herders.read().unwrap().keys().cloned().collect()
    }
}

impl std::fmt::Debug for MirrorMaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorMaker")
            .field("herders_count", &self.herders.read().unwrap().len())
            .field("start_latch", &self.start_latch.load(Ordering::SeqCst))
            .field("stop_latch", &self.stop_latch.load(Ordering::SeqCst))
            .field("shutdown", &self.shutdown.load(Ordering::SeqCst))
            .field("clusters", &self.clusters)
            .finish()
    }
}

// ============================================================================
// ShutdownHook - Internal shutdown handling
// ============================================================================

/// ShutdownHook - Handles graceful shutdown when process is terminated.
///
/// In Java, this is registered with Exit.addShutdownHook().
/// In Rust, equivalent functionality would use ctrlc crate or signal handling.
///
/// Corresponds to Java: MirrorMaker.ShutdownHook (inner class)
pub struct ShutdownHook {
    mirror_maker: Arc<RwLock<MirrorMaker>>,
}

impl ShutdownHook {
    /// Creates a new shutdown hook for the given MirrorMaker.
    pub fn new(mirror_maker: Arc<RwLock<MirrorMaker>>) -> Self {
        ShutdownHook { mirror_maker }
    }

    /// Executes the shutdown hook.
    ///
    /// Waits for startup to complete (with timeout) then stops the MirrorMaker.
    ///
    /// Corresponds to Java: ShutdownHook.run()
    pub fn run(&self) {
        // Wait for startup to complete with timeout
        let deadline = std::time::Instant::now() + Duration::from_secs(SHUTDOWN_TIMEOUT_SECONDS);
        while self
            .mirror_maker
            .read()
            .unwrap()
            .start_latch
            .load(Ordering::SeqCst)
            > 0
        {
            if std::time::Instant::now() >= deadline {
                eprintln!(
                    "Timed out in shutdown hook waiting for MirrorMaker startup to finish. Unable to shutdown cleanly."
                );
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }

        // Stop the MirrorMaker
        let mut mm = self.mirror_maker.write().unwrap();
        mm.stop();
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
        props.insert("B->A.enabled".to_string(), "true".to_string());
        MirrorMakerConfig::new(props)
    }

    #[test]
    fn test_mirror_maker_new() {
        let config = create_test_config();
        let mm = MirrorMaker::new(config, None);

        // Should have 2 herders (A->B and B->A)
        assert_eq!(mm.herder_count(), 2);
        assert!(mm.clusters().contains("A"));
        assert!(mm.clusters().contains("B"));
        assert!(!mm.is_shutdown());
    }

    #[test]
    fn test_mirror_maker_new_with_specific_clusters() {
        let config = create_test_config();
        let mm = MirrorMaker::new(config, Some(vec!["B".to_string()]));

        // Should only have herders targeting B (A->B)
        assert_eq!(mm.herder_count(), 1);
        assert!(mm.clusters().contains("B"));
        assert!(!mm.clusters().contains("A"));
    }

    #[test]
    fn test_mirror_maker_start() {
        let config = create_test_config();
        let mut mm = MirrorMaker::new(config, None);

        mm.start();

        // Start latch should be 0 (all started)
        assert_eq!(mm.start_latch.load(Ordering::SeqCst), 0);
        // Stop latch should be herder_count
        assert_eq!(mm.stop_latch.load(Ordering::SeqCst), mm.herder_count());
    }

    #[test]
    fn test_mirror_maker_stop() {
        let config = create_test_config();
        let mut mm = MirrorMaker::new(config, None);

        mm.start();
        mm.stop();

        // Should be shutdown
        assert!(mm.is_shutdown());
        // Stop latch should be 0 (all stopped)
        assert_eq!(mm.stop_latch.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_mirror_maker_stop_twice() {
        let config = create_test_config();
        let mut mm = MirrorMaker::new(config, None);

        mm.start();
        mm.stop();
        mm.stop(); // Should not error, just return early

        assert!(mm.is_shutdown());
    }

    #[test]
    fn test_mirror_maker_await_stop() {
        let config = create_test_config();
        let mut mm = MirrorMaker::new(config, None);

        mm.start();
        mm.stop();

        // Should complete quickly since herders are already stopped
        let result = mm.await_stop(Duration::from_secs(5));
        assert!(result);
    }

    #[test]
    fn test_mirror_maker_connector_status() {
        let config = create_test_config();
        let mm = MirrorMaker::new(config, None);

        let pair = SourceAndTarget::new("A", "B");
        let status = mm.connector_status(&pair, "test-connector");
        assert!(status.is_some());
    }

    #[test]
    fn test_mirror_maker_connector_status_invalid_pair() {
        let config = create_test_config();
        let mm = MirrorMaker::new(config, None);

        // Invalid pair (not in config)
        let pair = SourceAndTarget::new("X", "Y");
        let status = mm.connector_status(&pair, "test-connector");
        assert!(status.is_none());
    }

    #[test]
    fn test_mirror_maker_herder_pairs() {
        let config = create_test_config();
        let mm = MirrorMaker::new(config, None);

        let pairs = mm.herder_pairs();
        assert_eq!(pairs.len(), 2);
        assert!(pairs.contains(&SourceAndTarget::new("A", "B")));
        assert!(pairs.contains(&SourceAndTarget::new("B", "A")));
    }

    #[test]
    fn test_shutdown_hook() {
        let config = create_test_config();
        let mut mm = MirrorMaker::new(config, None);
        mm.start();

        let mm_arc = Arc::new(RwLock::new(mm));
        let hook = ShutdownHook::new(mm_arc.clone());

        hook.run();

        assert!(mm_arc.read().unwrap().is_shutdown());
    }

    #[test]
    #[should_panic(expected = "No source->target replication flows")]
    fn test_mirror_maker_new_empty_pairs() {
        let config = MirrorMakerConfig::default();
        let _mm = MirrorMaker::new(config, Some(vec!["X".to_string()]));
    }

    #[test]
    #[should_panic(expected = "MirrorMaker instance already started")]
    fn test_mirror_maker_double_start() {
        let config = create_test_config();
        let mut mm = MirrorMaker::new(config, None);

        mm.start();
        mm.start(); // Should panic
    }
}
